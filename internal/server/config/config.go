package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"go.yaml.in/yaml/v3"
)

type Config struct {
	Address           string        `yaml:"address"`
	WebOrigin         string        `yaml:"web_origin"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxMessageBytes   int           `yaml:"max_message_bytes"`
	Socket            struct {
		Enabled                   bool          `yaml:"enabled"`
		WriteTimeout              time.Duration `yaml:"write_timeout"`
		IdleTimeout               time.Duration `yaml:"idle_timeout"`
		MaxMessageBytes           int64         `yaml:"max_message_bytes"`
		MaxMessageDepth           int           `yaml:"max_message_depth"`
		Renewal                   RenewalConfig `yaml:"renewal"`
		MinimumClientRevision     uint64        `yaml:"minimum_client_revision"`
		RecommendedClientRevision uint64        `yaml:"recommended_client_revision"`
	} `yaml:"websocket"`
	Storage struct {
		Type     string `yaml:"type"`
		Postgres struct {
			URL                string              `yaml:"url"`
			Publication        string              `yaml:"publication"`
			Slot               string              `yaml:"slot"`
			AllowedTables      []string            `yaml:"allowed_tables"`
			QueryLimits        PostgresQueryLimits `yaml:"query_limits"`
			MessageLimits      MessageLimits       `yaml:"message_limits"`
			Replication        ReplicationConfig   `yaml:"replication"`
			CaptureBufferLimit int                 `yaml:"capture_buffer_limit"`
			MessageLogInterval time.Duration       `yaml:"message_log_interval"`
		} `yaml:"postgres"`
	} `yaml:"storage"`
	Auth       AuthConfig `yaml:"auth"`
	Visibility struct {
		Mode string `yaml:"mode"`
	} `yaml:"visibility"`
	Relations struct {
		File                string         `yaml:"file"`
		Limits              RelationLimits `yaml:"limits"`
		MaxFactRows         int            `yaml:"max_fact_rows"`
		MaxFactQueries      int            `yaml:"max_fact_queries"`
		FactLoadConcurrency int            `yaml:"fact_load_concurrency"`
	} `yaml:"relations"`
	Query      QueryConfig      `yaml:"query"`
	Projection ProjectionConfig `yaml:"projection"`
}

// RenewalConfig opts WebSockets into finite JWT-backed authentication leases.
// ExcludedClaims are credential metadata, never authorization inputs.
type RenewalConfig struct {
	Enabled                  bool          `yaml:"enabled"`
	ValidationTimeout        time.Duration `yaml:"validation_timeout"`
	MaxConcurrentValidations int           `yaml:"max_concurrent_validations"`
	ExcludedClaims           []string      `yaml:"excluded_claims"`
}

type AuthConfig struct {
	Mode        string     `yaml:"mode"`
	FixedClaims JSONClaims `yaml:"fixed_claims"`
	// ClaimTypes converts explicitly named top-level claims at admission.
	ClaimTypes    map[string]string `yaml:"claim_types"`
	Issuer        string            `yaml:"issuer"`
	Audience      string            `yaml:"audience"`
	ClockSkew     time.Duration     `yaml:"clock_skew"`
	HTTPTimeout   time.Duration     `yaml:"http_timeout"`
	MaxTokenBytes int               `yaml:"max_token_bytes"`
}

const ClaimTypeUUID = "uuid"
const ClaimTypeUUIDList = "list<uuid>"

func Default() Config {
	var c Config
	c.Address = "127.0.0.1:8080"
	c.Socket.Enabled = true
	c.ShutdownTimeout, c.ReadHeaderTimeout = 10*time.Second, 5*time.Second
	c.IdleTimeout = 75 * time.Second
	c.MaxMessageBytes = 8 << 20
	socket := projectionws.DefaultSocketConfig()
	c.Socket.WriteTimeout, c.Socket.IdleTimeout = socket.WriteTimeout, socket.IdleTimeout
	c.Socket.MaxMessageBytes, c.Socket.MaxMessageDepth = socket.MaxMessageBytes, socket.MaxMessageDepth
	authentication := projectionws.DefaultAuthenticationOptions()
	c.Socket.Renewal.ValidationTimeout = authentication.ValidationTimeout
	c.Socket.Renewal.MaxConcurrentValidations = authentication.MaxConcurrentValidations
	c.Socket.Renewal.ExcludedClaims = []string{"exp", "iat", "nbf", "jti"}
	c.Storage.Type = "postgres"
	c.Auth.HTTPTimeout, c.Auth.ClockSkew = 5*time.Second, time.Minute
	c.Auth.MaxTokenBytes = authentication.MaxTokenBytes
	c.setTuningDefaults()
	return c
}

// DecodeYAML rejects misspelled keys, duplicate keys, and extra documents. Policy
// and config are operator-owned startup inputs, not a dynamic control channel.
func DecodeYAML(data []byte, target any) error {
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("expected a single YAML document")
	}
	return nil
}

func Load(path string, lookup func(string) (string, bool)) (Config, error) {
	c := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return c, fmt.Errorf("read config: %w", err)
		}
		if err := DecodeYAML(data, &c); err != nil {
			return c, fmt.Errorf("decode config: %w", err)
		}
	}
	for name, value := range map[string]*string{
		"WEAVE_ADDRESS":    &c.Address,
		"WEAVE_WEB_ORIGIN": &c.WebOrigin, "WEAVE_STORAGE_TYPE": &c.Storage.Type,
		"WEAVE_DATABASE_URL": &c.Storage.Postgres.URL,
		"WEAVE_PUBLICATION":  &c.Storage.Postgres.Publication,
		"WEAVE_SLOT":         &c.Storage.Postgres.Slot,
		"WEAVE_AUTH_MODE":    &c.Auth.Mode,
		"WEAVE_JWT_ISSUER":   &c.Auth.Issuer, "WEAVE_JWT_AUDIENCE": &c.Auth.Audience,
		"WEAVE_VISIBILITY_MODE": &c.Visibility.Mode, "WEAVE_RELATIONS_FILE": &c.Relations.File,
	} {
		if override, ok := lookup(name); ok {
			*value = override
		}
	}
	if raw, ok := lookup("WEAVE_FIXED_CLAIMS"); ok {
		if err := c.Auth.FixedClaims.decode([]byte(raw)); err != nil {
			return c, fmt.Errorf("WEAVE_FIXED_CLAIMS: %w", err)
		}
	}
	if c.Relations.File != "" && !filepath.IsAbs(c.Relations.File) && path != "" {
		c.Relations.File = filepath.Join(filepath.Dir(path), c.Relations.File)
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Socket.RecommendedClientRevision != 0 && c.Socket.RecommendedClientRevision < c.Socket.MinimumClientRevision {
		return errors.New("websocket.recommended_client_revision must be zero or at least minimum_client_revision")
	}
	if c.Storage.Type != "postgres" {
		return fmt.Errorf("unsupported storage type %q", c.Storage.Type)
	}
	pg := c.Storage.Postgres
	if pg.URL == "" || pg.Publication == "" || pg.Slot == "" {
		return errors.New("postgres requires url, publication, and slot")
	}
	if c.Visibility.Mode != "allow_all" && c.Visibility.Mode != "definitions" {
		return errors.New("visibility.mode must be explicitly set to allow_all or definitions")
	}
	if c.Visibility.Mode == "definitions" && c.Relations.File == "" {
		return errors.New("relations.file is required in definitions visibility mode")
	}
	if err := c.Auth.Validate(); err != nil {
		return err
	}
	if err := c.validateRenewal(); err != nil {
		return err
	}
	if c.Address == "" {
		return errors.New("address is required")
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}

	if c.ShutdownTimeout <= 0 || c.ReadHeaderTimeout <= 0 || c.IdleTimeout <= 0 || c.MaxMessageBytes <= 0 || c.Socket.WriteTimeout <= 0 || c.Socket.IdleTimeout <= 0 || c.Socket.MaxMessageBytes <= 0 || c.Socket.MaxMessageDepth <= 0 {
		return errors.New("server timeouts and message limits must be positive")
	}
	return c.validateTuning()
}

func (c Config) validateRenewal() error {
	r := c.Socket.Renewal
	if r.ValidationTimeout <= 0 || r.MaxConcurrentValidations <= 0 {
		return errors.New("websocket.renewal validation_timeout and max_concurrent_validations must be positive")
	}
	seen := make(map[string]bool, len(r.ExcludedClaims))
	for _, name := range r.ExcludedClaims {
		if name == "" || seen[name] {
			return errors.New("websocket.renewal.excluded_claims requires unique nonempty claim names")
		}
		seen[name] = true
	}
	if r.Enabled {
		if !c.Socket.Enabled {
			return errors.New("websocket.renewal requires websocket.enabled")
		}
		if c.Auth.Mode == "fixed" {
			return errors.New("websocket.renewal requires oidc or unverified_jwt authentication, not fixed claims")
		}
		if _, mapped := c.Auth.ClaimTypes["exp"]; mapped {
			return errors.New("websocket.renewal requires a numeric exp claim; it cannot have a claim_types mapping")
		}
	}
	return nil
}

func (c AuthConfig) Validate() error {
	for name, kind := range c.ClaimTypes {
		if name == "" {
			return errors.New("auth.claim_types requires nonempty claim names")
		}
		switch kind {
		case ClaimTypeUUID, ClaimTypeUUIDList:
		default:
			return fmt.Errorf("unsupported auth.claim_types type %q for %q (supported: uuid, list<uuid>)", kind, name)
		}
	}
	if c.MaxTokenBytes <= 0 {
		return errors.New("auth.max_token_bytes must be positive")
	}
	switch c.Mode {
	case "fixed", "unverified_jwt":
		if c.Issuer != "" || c.Audience != "" {
			return errors.New("issuer and audience require oidc mode")
		}
	case "oidc":
		u, err := url.Parse(c.Issuer)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
			return errors.New("auth.issuer must be an absolute HTTP(S) issuer URL")
		}
		if c.Audience == "" {
			return errors.New("oidc mode requires audience")
		}
		if c.ClockSkew <= 0 || c.HTTPTimeout <= 0 {
			return errors.New("OIDC timeout and clock_skew must be positive")
		}
	default:
		return errors.New("auth.mode must be explicitly set to oidc, unverified_jwt, or fixed")
	}
	if c.Mode != "fixed" && c.FixedClaims != nil {
		return errors.New("fixed_claims requires fixed mode")
	}
	return nil
}

// yaml.Node preserves number spelling and disables YAML's implicit timestamp
// conversion. Converting via JSON also enforces string keys and JSON value types.
type JSONClaims map[string]any

func (o *JSONClaims) decode(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value map[string]any
	if err := d.Decode(&value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON object or null")
	}
	*o = value
	return nil
}
func (o *JSONClaims) UnmarshalYAML(n *yaml.Node) error {
	v, err := yamlJSON(n)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return o.decode(raw)
}
func yamlJSON(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.MappingNode:
		result := map[string]any{}
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Tag != "!!str" {
				return nil, errors.New("object keys must be strings")
			}
			if _, exists := result[key.Value]; exists {
				return nil, fmt.Errorf("duplicate key %q", key.Value)
			}
			v, err := yamlJSON(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			result[key.Value] = v
		}
		return result, nil
	case yaml.SequenceNode:
		result := make([]any, len(n.Content))
		for i, child := range n.Content {
			v, err := yamlJSON(child)
			if err != nil {
				return nil, err
			}
			result[i] = v
		}
		return result, nil
	case yaml.ScalarNode:
		switch n.Tag {
		case "!!null":
			return nil, nil
		case "!!str", "!!timestamp":
			return n.Value, nil
		case "!!bool":
			var value bool
			if err := n.Decode(&value); err != nil {
				return nil, err
			}
			return value, nil
		case "!!int", "!!float":
			// Only JSON decimal notation; no implicit hex, octal, NaN, infinity.
			text := strings.TrimSpace(n.Value)
			if !json.Valid([]byte(text)) {
				return nil, fmt.Errorf("use JSON number syntax for %q", text)
			}
			return json.Number(text), nil
		}
	}
	return nil, errors.New("unsupported YAML value (aliases and custom tags are not supported)")
}
