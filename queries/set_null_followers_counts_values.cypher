// This query selects up to 1000 users with a null follower count, counts their
// followers, updates the follower count, and returns the user’s public key
// along with the updated count.

 MATCH (user:User)
 WHERE user.follower_count IS NULL
 WITH user
 LIMIT 1000
 OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
 WITH user, COUNT(follower) AS follower_count
 SET user.follower_count = follower_count
 RETURN user.pubkey, user.follower_count;
