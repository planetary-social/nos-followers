CREATE TABLE follows (
    followee TEXT NOT NULL,
    follower TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (followee, follower)
);

CREATE INDEX follows_follower_idx ON follows (follower);
CREATE INDEX follows_followee_idx ON follows (followee);