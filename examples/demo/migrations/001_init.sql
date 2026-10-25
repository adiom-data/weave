CREATE SCHEMA tenant_001;

CREATE TABLE tenant_001.organizations (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT organizations_name_not_empty CHECK (length(btrim(name)) > 0)
);

CREATE TABLE tenant_001.users (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT users_organization_fk FOREIGN KEY (organization_id) REFERENCES tenant_001.organizations (id),
    CONSTRAINT users_organization_identity UNIQUE (organization_id, id),
    CONSTRAINT users_display_name_not_empty CHECK (length(btrim(display_name)) > 0)
);

CREATE TABLE tenant_001.teams (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT teams_organization_fk FOREIGN KEY (organization_id) REFERENCES tenant_001.organizations (id),
    CONSTRAINT teams_organization_identity UNIQUE (organization_id, id),
    CONSTRAINT teams_name_not_empty CHECK (length(btrim(name)) > 0)
);

CREATE TABLE tenant_001.user_teams (
    organization_id UUID NOT NULL,
    user_id UUID NOT NULL,
    team_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, user_id, team_id),
    CONSTRAINT user_teams_user_fk FOREIGN KEY (organization_id, user_id) REFERENCES tenant_001.users (organization_id, id),
    CONSTRAINT user_teams_team_fk FOREIGN KEY (organization_id, team_id) REFERENCES tenant_001.teams (organization_id, id)
);

CREATE TABLE tenant_001.projects (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    title TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT projects_organization_fk FOREIGN KEY (organization_id) REFERENCES tenant_001.organizations (id),
    CONSTRAINT projects_organization_identity UNIQUE (organization_id, id),
    CONSTRAINT projects_title_not_empty CHECK (length(btrim(title)) > 0)
);

CREATE TABLE tenant_001.tasks (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT tasks_organization_fk FOREIGN KEY (organization_id) REFERENCES tenant_001.organizations (id),
    CONSTRAINT tasks_organization_identity UNIQUE (organization_id, id),
    CONSTRAINT tasks_title_not_empty CHECK (length(btrim(title)) > 0)
);

CREATE TABLE tenant_001.project_tasks (
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    task_id UUID NOT NULL,
    position NUMERIC(30, 10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, project_id, task_id),
    CONSTRAINT project_tasks_project_fk FOREIGN KEY (organization_id, project_id) REFERENCES tenant_001.projects (organization_id, id),
    CONSTRAINT project_tasks_task_fk FOREIGN KEY (organization_id, task_id) REFERENCES tenant_001.tasks (organization_id, id),
    CONSTRAINT project_tasks_position_finite CHECK (position NOT IN ('Infinity', '-Infinity', 'NaN'))
);

CREATE INDEX project_tasks_project_order
    ON tenant_001.project_tasks (organization_id, project_id, position, task_id)
    WHERE deleted_at IS NULL;

CREATE TABLE tenant_001.project_user_grants (
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, project_id, user_id),
    CONSTRAINT project_user_grants_project_fk FOREIGN KEY (organization_id, project_id) REFERENCES tenant_001.projects (organization_id, id),
    CONSTRAINT project_user_grants_user_fk FOREIGN KEY (organization_id, user_id) REFERENCES tenant_001.users (organization_id, id)
);

CREATE INDEX project_user_grants_user_lookup
    ON tenant_001.project_user_grants (organization_id, user_id, project_id)
    WHERE deleted_at IS NULL;

CREATE TABLE tenant_001.project_team_grants (
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    team_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (organization_id, project_id, team_id),
    CONSTRAINT project_team_grants_project_fk FOREIGN KEY (organization_id, project_id) REFERENCES tenant_001.projects (organization_id, id),
    CONSTRAINT project_team_grants_team_fk FOREIGN KEY (organization_id, team_id) REFERENCES tenant_001.teams (organization_id, id)
);

CREATE INDEX project_team_grants_team_lookup
    ON tenant_001.project_team_grants (organization_id, team_id, project_id)
    WHERE deleted_at IS NULL;

INSERT INTO tenant_001.organizations (id, name)
VALUES ('00000000-0000-4000-8000-000000000001', 'Adiom Weave Demo');

INSERT INTO tenant_001.users (id, organization_id, display_name)
VALUES ('20000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000001', 'Demo User');

INSERT INTO tenant_001.teams (id, organization_id, name)
VALUES ('30000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000001', 'Demo Team');

INSERT INTO tenant_001.user_teams (organization_id, user_id, team_id)
VALUES ('00000000-0000-4000-8000-000000000001', '20000000-0000-4000-8000-000000000001', '30000000-0000-4000-8000-000000000001');

INSERT INTO tenant_001.projects (id, organization_id, title)
VALUES ('10000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000001', 'Build the reactive service');

INSERT INTO tenant_001.project_team_grants (organization_id, project_id, team_id)
VALUES ('00000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '30000000-0000-4000-8000-000000000001');

-- ALLOWED_TABLES may restrict client queries. All demo tables are published so
-- a future policy controller can consume authorization facts in commit order.
CREATE PUBLICATION reactive_queries FOR ALL TABLES;
