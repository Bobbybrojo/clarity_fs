mod utility;
mod client;

use std::sync::Arc;
use uuid::Uuid;
use iced::Alignment::Center;
use tokio::sync::Mutex;
use utility::Utility;
use client::{Client, RoomInfo};

use iced::{Font, Shadow, Task, Theme, Vector, window};
use iced::Length::{Fill, FillPortion};
use iced::{Border, Element, Color};
use iced::widget::{button, column, Column, container, row, rule, scrollable, text, text_input};


#[derive(Default, Clone)]
enum Screen {
    #[default]
    Enter,
    Menu,
}

struct MainState {
    screen: Screen,
    theme: Theme,
    client: Option<Arc<Mutex<Client>>>,
    rooms: Vec<RoomInfo>,
    current_room: Uuid,
}

impl MainState {
    fn new() -> Self {
        MainState {
            screen: Screen::Enter,
            theme: Theme::Dark,
            client: None,
            rooms: Vec::new(),
            current_room: Uuid::default(),
        }
    }
}

#[derive(Clone)]
enum Message {
    EnterApp,
    ClientCreated(Option<Arc<Mutex<Client>>>),
    UpdateRoomList(Option<Vec<RoomInfo>>),
    JoinRoom(Uuid),
    EnterRoom(Uuid),
    EndTasks
}


fn update(state: &mut MainState, message: Message) -> Task<Message> {
    match message {

        Message::EnterApp => {
            state.screen = Screen::Menu;
            Task::perform(
                client::Client::new(),
                Message::ClientCreated
            )
        }

        Message::ClientCreated(client) => {
            state.client = client.clone();
            Task::perform(
                async move {
                    if let Some(c) = client.clone() {
                        let c_guard = c.lock().await;
                        Some(c_guard.rooms.clone())
                    } else {
                        None
                    }
                },
                Message::UpdateRoomList
            )
        }

        Message::UpdateRoomList(some_rooms) => {
            if let Some(rooms) = some_rooms {
                state.rooms = rooms;
            };
            Task::none()
        }

        Message::JoinRoom(rid) => {
            let client = state.client.clone();
            Task::perform(
                async move {
                    if let Some(c) = client {
                        let mut c_guard = c.lock().await;
                        c_guard.join_room(rid).await;
                    }
                },
                move |_| Message::EnterRoom(rid)
            )
        }

        Message::EnterRoom(rid) => {
            state.current_room = rid;
            Task::none()
        }

        Message::EndTasks => {
            Task::none()
        }


    }
}

fn view(state: &MainState) -> Element<'_, Message> {
    let screen_element: Element<'_, Message>;
    match state.screen {

        Screen::Enter => {
            screen_element = container(
                column![
                    text("Clarity").size(64),
                    button("Enter")
                        .on_press(Message::EnterApp)
                        .style(|_, _| button::Style {
                            background: Some(iced::Background::Color(Utility::accent().into())),
                            text_color: Color::WHITE,
                            border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                            ..Default::default()
                        })
                ].align_x(Center)
            ).center_x(Fill).into()
        }

        Screen::Menu => {
            screen_element = container(
                column![
                    text("Clarity").size(64),
                    row![
                        column![].width(FillPortion(1)),
                        column![
                            scrollable(
                                Column::with_children(
                                    state.rooms.iter().map(|room: &RoomInfo| -> Element<'_, Message> {
                                        container(
                                            row![
                                                text(format!("Room: {}", room.rid.clone().to_string()))
                                                    .width(iced::Length::Fill),
                                                button(column![
                                                        text(format!("People: {}", room.peer_count)), 
                                                        text("Join")
                                                    ].align_x(Center)
                                                ).style(|_, _| button::Style {
                                                    background: Some(iced::Background::Color(Utility::accent().into())),
                                                    text_color: Color::WHITE,
                                                    border: Border { color: Color::TRANSPARENT, width: 1.0, radius: 8.0.into() },
                                                    ..Default::default()
                                                }).padding(8)
                                                .on_press(Message::JoinRoom(room.rid)),
                                            ]
                                        )
                                        .style(|_| container::Style{
                                            background: Some(Utility::darker().into()),
                                            border: Border{
                                                color: Color::TRANSPARENT,
                                                width: 5.0,
                                                radius: 18.into(),
                                            },
                                            ..Default::default()
                                        })
                                        .padding(8)
                                        .align_top(65)
                                        .width(iced::Length::Fill)
                                        .center_y(Fill)
                                        .into()
                                    })
                                )
                            )
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