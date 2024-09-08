// This query selects up to 1000 users with a null follower count, counts their
// followers and updates the follower count.

MATCH (user:User)
WHERE user.follower_count IS NULL
WITH user
LIMIT 10000
OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
WITH user, COUNT(follower) AS follower_count
SET user.follower_count = follower_count;

// Check how many still are null

MATCH (user:User)
WHERE user.follower_count IS NULL
RETURN COUNT(user) AS num_users_with_null_follower_count;

// Ensure the cache is correct for a particular pubkey

MATCH (user:User {pubkey: "89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5"})
OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
WITH user, COUNT(follower) AS fresh_follower_count, user.follower_count AS cached_follower_count
RETURN user.pubkey, fresh_follower_count, cached_follower_count;
