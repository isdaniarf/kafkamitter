use security_framework::passwords::{delete_generic_password, get_generic_password, set_generic_password};

const SERVICE: &str = "kafkamitter";
const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;

pub fn set_password(profile_id: &str, password: &str) -> anyhow::Result<()> {
    set_generic_password(SERVICE, profile_id, password.as_bytes())?;
    Ok(())
}

pub fn get_password(profile_id: &str) -> anyhow::Result<Option<String>> {
    match get_generic_password(SERVICE, profile_id) {
        Ok(bytes) => Ok(Some(String::from_utf8(bytes)?)),
        Err(err) if err.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn delete_password(profile_id: &str) -> anyhow::Result<()> {
    match delete_generic_password(SERVICE, profile_id) {
        Ok(()) => Ok(()),
        Err(err) if err.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrites_an_existing_password() {
        let account = format!("kafkamitter-test-{}", std::process::id());
        set_password(&account, "first").expect("first write");
        let second = set_password(&account, "second");
        let read = get_password(&account);
        let _ = delete_password(&account);
        second.expect("second write must succeed");
        assert_eq!(read.unwrap().as_deref(), Some("second"));
    }
}
