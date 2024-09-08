// Looks for users with more than 100 followers that have too many followers with very few followees

MATCH (suspect:User)-[f:FOLLOWS]->(target:User)
WITH target, COUNT(DISTINCT suspect) AS num_followers
WHERE num_followers > 100
WITH target, num_followers
ORDER BY num_followers DESC
LIMIT 10
MATCH (suspect:User)-[f:FOLLOWS]->(target:User)
MATCH (suspect)-[:FOLLOWS]->(others:User)
WITH target, num_followers, suspect, COUNT(DISTINCT others) AS num_followees
WHERE num_followees < 3
WITH target, num_followers, COUNT(DISTINCT suspect) AS num_suspects
ORDER BY num_suspects DESC
RETURN target, num_followers, num_suspects
LIMIT 10;
