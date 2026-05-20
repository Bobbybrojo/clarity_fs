use iced::{color, Color};

pub struct Utility {}

impl Utility {
    pub fn window_background() -> Color {
        return color!(30, 30, 30, 0.7);
    }

    pub fn darkest() -> Color {
        return color!(30, 30, 30);
    }

    pub fn darker() -> Color {
        return color!(37, 37, 38);
    }

    pub fn dark() -> Color {
        return color!(45, 45, 48);
    }

    pub fn gray() -> Color {
        return color!(62, 62, 66);
    }

    pub fn accent() -> Color {
        return color!(0, 122, 224);
    }
}