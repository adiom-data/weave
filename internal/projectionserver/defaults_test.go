package projectionserver

import (
	transactionmessages "github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"time"
)

func newWithDefaults(queries querysubscription.Client) *server { return newServer(queries, Options{}) }

func newServerWithRouter(queries querysubscription.Client, router *transactionmessages.Router) *server {
	s := newServer(queries, Options{MessageProgressInterval: time.Millisecond})
	s.router = router
	return s
}
