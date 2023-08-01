use crate::output::yellow_bold_spec;

pub trait ColorWriter {
    fn warn_line(&mut self, msg: &str) -> std::io::Result<()>;
}

pub struct NoopColorWriter;

impl std::io::Write for NoopColorWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl termcolor::WriteColor for NoopColorWriter {
    fn supports_color(&self) -> bool {
        false
    }

    fn set_color(&mut self, _: &termcolor::ColorSpec) -> std::io::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<T: termcolor::WriteColor> ColorWriter for T {
    fn warn_line(&mut self, message: &str) -> std::io::Result<()> {
        self.set_color(&yellow_bold_spec())?;
        self.write_all(b"WARNING: ")?;
        self.write_all(message.as_bytes())?;
        self.write_all(b"\n")?;
        self.reset()?;
        Ok(())
    }
}
