pub fn trim_whitespace(buf: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = buf.len();
    while start < end && buf[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && buf[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &buf[start..end]
}

#[cfg(test)]
mod test {
    use super::trim_whitespace;

    #[test]
    fn test_trim_whitespace() {
        assert_eq!(trim_whitespace(b"  hello world  "), b"hello world");
        assert_eq!(trim_whitespace(b"  hello world"), b"hello world");
        assert_eq!(trim_whitespace(b"hello world  "), b"hello world");
        assert_eq!(trim_whitespace(b"hello world"), b"hello world");
        assert_eq!(trim_whitespace(b"  "), b"");
        assert_eq!(trim_whitespace(b""), b"");
    }
}
