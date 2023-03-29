pub fn is_blank(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }

    for &byte in bytes.iter() {
        if !byte.is_ascii_whitespace() {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod test {
    #[test]
    fn test_is_blank() {
        assert!(super::is_blank(b""));
        assert!(super::is_blank(b" "));
        assert!(super::is_blank(b"  "));
        assert!(super::is_blank(b" \t"));
        assert!(super::is_blank(b" \n "));
    }

    #[test]
    fn test_is_not_blank() {
        assert!(!super::is_blank(b"a"));
        assert!(!super::is_blank(b" a"));
        assert!(!super::is_blank(b"a "));
        assert!(!super::is_blank(b" a "));
        assert!(!super::is_blank(b" a b "));
    }
}
