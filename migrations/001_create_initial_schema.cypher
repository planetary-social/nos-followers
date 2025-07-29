CREATE CONSTRAINT user_pubkey_unique IF NOT EXISTS FOR (u:User) REQUIRE u.pubkey IS UNIQUE;
CREATE INDEX follows_created_at_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.created_at);
CREATE INDEX follows_updated_at_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.updated_at);
