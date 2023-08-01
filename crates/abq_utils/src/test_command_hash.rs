use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub struct TestCommandHash([u8; 32]);

impl TestCommandHash {
    pub fn from_command(command: &str, args: &[String]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(command.as_bytes());
        for arg in args {
            hasher.update(arg.as_bytes());
        }
        Self(hasher.finalize().into())
    }

    pub fn random() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&rand::random::<[u8; 32]>());
        Self(hasher.finalize().into())
    }
}

#[cfg(test)]
mod test {
    use super::TestCommandHash;

    #[test]
    fn random_is_random() {
        let one = TestCommandHash::random();
        let two = TestCommandHash::random();
        assert_ne!(one, two);
    }

    #[test]
    fn consistent_for_same_command() {
        let one = TestCommandHash::from_command("foo", &["arg1".into()]);
        let two = TestCommandHash::from_command("foo", &["arg1".into()]);
        assert_eq!(one, two);
    }

    #[test]
    fn different_for_different_command() {
        let one = TestCommandHash::from_command("foo", &["arg1".into()]);
        let two = TestCommandHash::from_command("bar", &["arg1".into()]);
        assert_ne!(one, two);
    }

    #[test]
    fn different_for_different_args() {
        let one = TestCommandHash::from_command("foo", &["arg1".into()]);
        let two = TestCommandHash::from_command("foo", &["arg2".into()]);
        assert_ne!(one, two);
    }
}
