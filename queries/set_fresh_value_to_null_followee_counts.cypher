// This query selects up to 1000 users with a null followee count, counts their
// followees and updates the followee count.

MATCH (user:User)
WHERE user.followee_count IS NULL
WITH user
LIMIT 1000
OPTIONAL MATCH (user)-[:FOLLOWS]->(followee:User)
WITH user, COUNT(followee) AS followee_count
SET user.followee_count = followee_count;

// This query sets the followee_count to null for all users.

MATCH (user:User)
WHERE user.followee_count IS NOT NULL
SET user.followee_count = null;

// Check how many still have a non-null followee_count (should return 0 if all were set to null)

MATCH (user:User)
WHERE user.followee_count IS NOT NULL
RETURN COUNT(user) AS num_users_with_non_null_followee_count;

// Check how many still are null

MATCH (user:User)
WHERE user.followee_count IS NULL
RETURN COUNT(user) AS num_users_with_null_followee_count;

// Ensure the cache is correct for a particular pubkey

MATCH (user:User {pubkey: "89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5"})
OPTIONAL MATCH (user)-[:FOLLOWS]->(followee:User)
WITH user, COUNT(followee) AS fresh_followee_count, user.followee_count AS cached_followee_count
RETURN user.pubkey, fresh_followee_count, cached_followee_count;
