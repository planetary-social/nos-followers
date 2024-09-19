use chrono::{DateTime, Utc};

pub fn is_trusted(
    has_quality_followers: bool,
    _followed_by_enough_people: bool,
    _following_enough_people: bool,
    _oldest_event_seen_at: DateTime<Utc>,
    _latest_contact_list_at: DateTime<Utc>,
    _is_nos_user: bool,
) -> bool {
    if _is_nos_user {
        return true;
    }

    if has_quality_followers {
        return true;
    }

    // if followed_by_enough_people && following_enough_people {
    //     return false;
    // }

    return false;
}
