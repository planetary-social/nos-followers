// Find users that share a similar set of followees

MATCH (target:User {pubkey: '89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5'})-[:FOLLOWS]->(commonFollowee:User)
WITH target, commonFollowee

// Find other users who follow the same followees
MATCH (otherUser:User)-[:FOLLOWS]->(commonFollowee)
WHERE otherUser <> target  // Exclude the target user from comparison
WITH target, otherUser, COUNT(DISTINCT commonFollowee) AS sharedFolloweesCount

// Use the cached followee_count of the target user
WITH otherUser, sharedFolloweesCount, target.followee_count AS totalFolloweesCount

// Calculate the percentage of shared followees
WITH otherUser, sharedFolloweesCount, totalFolloweesCount, (toFloat(sharedFolloweesCount) / totalFolloweesCount) AS sharedPercentage

// Ensure distinct otherUser and format the output with njump link and friendly_id
WITH DISTINCT otherUser, sharedFolloweesCount, sharedPercentage
RETURN 'https://njump.me/' + otherUser.friendly_id AS clickable_link, otherUser.pubkey, sharedFolloweesCount, sharedPercentage
ORDER BY sharedPercentage DESC
LIMIT 5;
