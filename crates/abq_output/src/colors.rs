pub struct ColorProvider {
    pub green_bold: &'static str,
    pub red_bold: &'static str,
    pub bold: &'static str,
    pub reset: &'static str,
}

impl ColorProvider {
    pub const ANSI: Self = Self {
        green_bold: "\x1B[32;1m",
        red_bold: "\x1B[31;1m",
        bold: "\x1B[1m",
        reset: "\x1B[0m",
    };

    pub const NOCOLOR: Self = Self {
        green_bold: "",
        red_bold: "",
        bold: "",
        reset: "",
    };

    pub const CMD: Self = Self {
        green_bold: "<green-bold>",
        red_bold: "<red-bold>",
        bold: "<bold>",
        reset: "<reset>",
    };
}
