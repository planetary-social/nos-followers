CREATE INDEX user_pagerank_idx IF NOT EXISTS FOR (u:User) ON (u.pagerank);
