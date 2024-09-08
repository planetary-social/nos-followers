// This query selects up to 1000 users with a null follower count, counts their
// followers and updates the follower count.

MATCH (user:User)
WHERE user.follower_count IS NULL
WITH user
LIMIT 10000
OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
WITH user, COUNT(follower) AS follower_count
SET user.follower_count = follower_count;

// This query sets the follower_count to null for all users.

MATCH (user:User)
WHERE user.follower_count IS NOT NULL
SET user.follower_count = null;

// Check how many still have a non-null follower_count (should return 0 if all were set to null)

MATCH (user:User)
WHERE user.follower_count IS NOT NULL
RETURN COUNT(user) AS num_users_with_non_null_follower_count;

// Check how many still are null

MATCH (user:User)
WHERE user.follower_count IS NULL
RETURN COUNT(user) AS num_users_with_null_follower_count;

// Ensure the cache is correct for a particular pubkey

MATCH (user:User {pubkey: "89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5"})
OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
WITH user, COUNT(follower) AS fresh_follower_count, user.follower_count AS cached_follower_count
RETURN user.pubkey, fresh_follower_count, cached_follower_count;
