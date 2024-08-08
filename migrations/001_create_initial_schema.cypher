CREATE CONSTRAINT FOR (u:User) REQUIRE u.pubkey IS UNIQUE;
CREATE INDEX follows_created_at_idx FOR ()-[r:FOLLOWS]-() ON (r.created_at);
CREATE INDEX follows_updated_at_idx FOR ()-[r:FOLLOWS]-() ON (r.updated_at);
