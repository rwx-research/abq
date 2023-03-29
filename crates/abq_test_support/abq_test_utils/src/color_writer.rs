use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct TestColorWriter<W: io::Write>(W);

impl<W: io::Write> TestColorWriter<W> {
    pub fn new(w: W) -> Self {
        Self(w)
    }

    pub fn get(self) -> W {
        self.0
    }
}

impl<W: io::Write> io::Write for TestColorWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<W: io::Write> termcolor::WriteColor for TestColorWriter<W> {
    fn supports_color(&self) -> bool {
        true
    }

    fn set_color(&mut self, spec: &termcolor::ColorSpec) -> io::Result<()> {
        assert!(spec.bg().is_none());
        assert!(!spec.intense());
        assert!(!spec.underline());
        assert!(!spec.dimmed());
        assert!(!spec.italic());
        assert!(spec.reset());

        use termcolor::Color::*;
        let mut spec_parts = vec![];
        if spec.bold() {
            spec_parts.push("bold")
        }
        if let Some(color) = spec.fg() {
            let co = match color {
                Black => "black",
                Blue => "blue",
                Green => "green",
                Red => "red",
                Cyan => "cyan",
                Magenta => "magenta",
                Yellow => "yellow",
                White => "white",
                _ => unreachable!(),
            };
            spec_parts.push(co);
        }

        write!(&mut self.0, "<{}>", spec_parts.join("-"))
    }

    fn reset(&mut self) -> io::Result<()> {
        write!(&mut self.0, "<reset>")
    }
}

#[derive(Clone)]
pub struct SharedTestColorWriter<W: io::Write>(Arc<Mutex<TestColorWriter<W>>>);

impl<W: io::Write> SharedTestColorWriter<W> {
    pub fn new(w: W) -> Self {
        Self(Arc::new(Mutex::new(TestColorWriter(w))))
    }
}

impl<W: io::Write + std::fmt::Debug> SharedTestColorWriter<W> {
    pub fn get(self) -> W {
        Arc::try_unwrap(self.0).unwrap().into_inner().unwrap().get()
    }
}

impl<W: io::Write> io::Write for SharedTestColorWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

impl<W: io::Write> termcolor::WriteColor for SharedTestColorWriter<W> {
    fn supports_color(&self) -> bool {
        self.0.lock().unwrap().supports_color()
    }

    fn set_color(&mut self, spec: &termcolor::ColorSpec) -> io::Result<()> {
        self.0.lock().unwrap().set_color(spec)
    }

    fn reset(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().reset()
    }
}
