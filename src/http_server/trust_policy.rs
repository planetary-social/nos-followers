pub fn is_trusted(found: bool, has_quality_followers: bool, _is_nos_user: bool) -> bool {
    if found && (_is_nos_user || has_quality_followers) {
        return true;
    }

    false
}
