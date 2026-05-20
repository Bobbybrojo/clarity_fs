mod utility;
use iced::Alignment::Center;
use utility::Utility;

use iced::{Font, Shadow, Task, Theme, Vector, window};
use iced::Length::{Fill, FillPortion};
use iced::{Border, Element, Color};
use iced::widget::{button, column, container, row, rule, scrollable, text, text_input};


#[derive(Default, Clone)]
enum Screen {
    #[default]
    Menu,
}

struct MainState {
    screen: Screen,
    theme: Theme,
}

impl MainState {
    fn new() -> Self {
        MainState {
            screen: Screen::Menu,
            theme: Theme::Dark,
        }
    }
}

#[derive(Debug, Clone)]
enum Message {
}


fn update(state: &mut MainState, message: Message) -> Task<Message> {
    Task::none()
}

fn view(state: &MainState) -> Element<'_, Message> {
    let screen_element: Element<'_, Message>;
    match state.screen {
        Screen::Menu => {
            screen_element = container(
                column![
                    text("Clarity").size(64),
                    row![
                        column![].width(FillPortion(1)),
                        column![
                            column![
                                text("Enter IP:"),
                                text_input("IP", "")
                            ].spacing(2),
                            row![
                                button("Host"),
                                rule::vertical(2),
                                button("Join"),
                            ].height(30)
                            .spacing(4)
                        ].spacing(16).width(FillPortion(2)),
                        column![].width(FillPortion(1)),
                    ],
                ].align_x(Center)
            )
            .padding(iced::Padding { top: 0.0, right: 12.0, bottom: 12.0, left: 12.0 } )
            .height(Fill)
            .into()
        }
    }

    column![
        // Title bar buffer
        container("").width(Fill).height(32),
        // Main application container
        container(
            // Text Editor
            screen_element
            
        ).style(|t: &Theme| {
            container::Style {
                text_color: Some(t.palette().text),
                background: None,
                border: Border {
                    color: Color::TRANSPARENT,
                    width: 0.0,
                    radius: 0.0.into(),
                },
                shadow: Shadow {
                    color: Color::TRANSPARENT,
                    offset: Vector::new(0.0, 0.0),
                    blur_radius: 0.0
                },
                snap: false,
            }
        })
    ].into()

    
}

pub fn main() -> iced::Result {
    iced::application(MainState::new, update, view)
    .style(|_, _theme| iced::theme::Style{
        background_color: Utility::window_background(),
        text_color: Color::WHITE,
    })
    .window(window::Settings { 
        size: (720, 480).into(), 
        maximized: false, 
        fullscreen: false, 
        position: window::Position::Default, 
        min_size: Some((720, 480).into()), 
        max_size: None, 
        visible: true, 
        resizable: true, 
        closeable: true, 
        minimizable: true, 
        decorations: true, 
        transparent: true,
        blur: true,
        level: window::Level::Normal, 
        icon: None, 
        platform_specific: window::settings::PlatformSpecific { 
            title_hidden: false, 
            titlebar_transparent: true, 
            fullsize_content_view: true 
        }, 
        exit_on_close_request: true 
    })
    .centered()
    .title("")
    .theme(Theme::Dark)
    .default_font(Font::DEFAULT)
    .run()
}