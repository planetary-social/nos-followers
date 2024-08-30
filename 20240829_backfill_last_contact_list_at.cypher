// Backfill script to set last_contact_list_at for User nodes where it's null

MATCH (u:User)
WHERE u.last_contact_list_at IS NULL
OPTIONAL MATCH (u)-[r:FOLLOWS]->()
WITH u, MAX(r.created_at) AS newest_contact_list_at
WHERE newest_contact_list_at IS NOT NULL
SET u.last_contact_list_at = newest_contact_list_at;
