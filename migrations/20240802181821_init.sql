-- Add migration script here
CREATE TABLE events (
    id UUID PRIMARY KEY,
    agg_id UUID NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE PUBLICATION events_pub
FOR TABLE events
WITH (publish = 'insert');


-- SELECT * FROM pg_create_logical_replication_slot('events_pub_slot', 'pgoutput');
CREATE_REPLICATION_SLOT events_pub_slot LOGICAL pgoutput;

-- CREATE SUBSCRIPTION events_sub
-- PUBLICATION events_pub;
