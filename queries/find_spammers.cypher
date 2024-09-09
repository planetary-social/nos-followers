// Looks for users with more than 50 followers that have too many followers with very few followees

MATCH (target:User)
WHERE target.follower_count > 50
WITH target, target.follower_count AS num_followers, target.followee_count AS num_followees
ORDER BY num_followers DESC


// Now find the suspects (followers of target) and check their followee_count
MATCH (suspect:User)-[f:FOLLOWS]->(target)
WHERE suspect.followee_count < 4
WITH target, num_followers, num_followees, COUNT(DISTINCT suspect) AS num_suspects
WITH target, num_followers, num_followees, num_suspects,
    TOFLOAT(num_suspects) / TOFLOAT(num_followers) AS suspect_ratio
ORDER BY suspect_ratio DESC
RETURN target, num_followers, num_followees, num_suspects, suspect_ratio
LIMIT 10;