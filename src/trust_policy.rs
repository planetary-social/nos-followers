/// Determines if a user is trusted based on various criteria
///
/// This is a lower-level function used by the AccountInfo.is_trusted method.
/// In most cases, you should use AccountInfo.is_trusted instead.
///
/// # Arguments
/// * `found` - Whether the user exists in the database
/// * `has_quality_followers` - Whether the user has a high enough pagerank
/// * `is_nos_user` - Whether the user is registered with Nos
///
/// # Returns
/// `true` if the user is trusted, `false` otherwise
pub fn is_trusted(found: bool, has_quality_followers: bool, is_nos_user: bool) -> bool {
    if found && (is_nos_user || has_quality_followers) {
        return true;
    }

    false
}
