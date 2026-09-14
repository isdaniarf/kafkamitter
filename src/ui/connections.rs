use std::rc::Rc;

use gpui::prelude::FluentBuilder as _;
use gpui::*;
use gpui_component::input::{Input, InputState};
use gpui_component::notification::Notification;
use gpui_component::select::{Select, SelectState};
use gpui_component::{ActiveTheme, IndexPath, WindowExt, v_flex};

use crate::app::KafkamitterApp;
use crate::ui::dialog_footer::ok_cancel_footer;
use crate::model::profile::{ConnectionProfile, SaslMechanism, Security, new_profile_id};

const SECURITY_OPTIONS: [&str; 2] = ["PLAINTEXT", "SASL_SSL"];

pub struct ConnectionForm {
    existing_id: Option<String>,
    name: Entity<InputState>,
    servers: Entity<InputState>,
    security: Entity<SelectState<Vec<SharedString>>>,
    mechanism: Entity<SelectState<Vec<SharedString>>>,
    username: Entity<InputState>,
    password: Entity<InputState>,
    ca_location: Entity<InputState>,
}

impl ConnectionForm {
    pub fn new(
        existing: Option<&ConnectionProfile>,
        password: Option<&str>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let (security_ix, mechanism_ix, username) = match existing.map(|p| &p.security) {
            Some(Security::SaslSsl {
                mechanism,
                username,
            }) => (
                1,
                SaslMechanism::ALL.iter().position(|m| m == mechanism).unwrap_or(0),
                username.clone(),
            ),
            _ => (0, 0, String::new()),
        };
        let mut text_input = |cx: &mut App, placeholder: &str, value: &str| {
            let placeholder = placeholder.to_string();
            let value = value.to_string();
            cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(placeholder)
                    .default_value(value)
            })
        };
        let name = text_input(cx, "Local cluster", existing.map_or("", |p| p.name.as_str()));
        let servers = text_input(
            cx,
            "localhost:9092",
            existing.map_or("", |p| p.bootstrap_servers.as_str()),
        );
        let username = text_input(cx, "username", &username);
        let ca_location = text_input(
            cx,
            "Optional path to a CA certificate file",
            existing.and_then(|p| p.ca_location.as_deref()).unwrap_or(""),
        );
        let password_value = password.unwrap_or("").to_string();
        let password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("password")
                .masked(true)
                .default_value(password_value)
        });
        let security = cx.new(|cx| {
            SelectState::new(
                SECURITY_OPTIONS.iter().map(|s| SharedString::from(*s)).collect::<Vec<_>>(),
                Some(IndexPath::default().row(security_ix)),
                window,
                cx,
            )
        });
        let mechanism = cx.new(|cx| {
            SelectState::new(
                SaslMechanism::ALL
                    .iter()
                    .map(|m| SharedString::from(m.label()))
                    .collect::<Vec<_>>(),
                Some(IndexPath::default().row(mechanism_ix)),
                window,
                cx,
            )
        });
        Self {
            existing_id: existing.map(|p| p.id.clone()),
            name,
            servers,
            security,
            mechanism,
            username,
            password,
            ca_location,
        }
    }

    pub fn set_name(&self, name: &str, window: &mut Window, cx: &mut App) {
        self.name.update(cx, |input, cx| input.set_value(name, window, cx));
    }

    fn uses_sasl(&self, cx: &App) -> bool {
        self.security
            .read(cx)
            .selected_value()
            .is_some_and(|v| v.as_ref() == "SASL_SSL")
    }

    pub fn render(&self, cx: &App) -> AnyElement {
        let uses_sasl = self.uses_sasl(cx);
        let field = |label: &'static str, control: AnyElement| {
            v_flex()
                .gap_1()
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(label),
                )
                .child(control)
        };
        v_flex()
            .gap_3()
            .w_full()
            .child(field("Name", Input::new(&self.name).w_full().into_any_element()))
            .child(field(
                "Bootstrap servers",
                Input::new(&self.servers).w_full().into_any_element(),
            ))
            .child(field(
                "Security",
                Select::new(&self.security).w_full().into_any_element(),
            ))
            .when(uses_sasl, |form| {
                form.child(field(
                    "SASL mechanism",
                    Select::new(&self.mechanism).w_full().into_any_element(),
                ))
                .child(field(
                    "Username",
                    Input::new(&self.username).w_full().into_any_element(),
                ))
                .child(field(
                    "Password",
                    Input::new(&self.password).w_full().into_any_element(),
                ))
                .child(field(
                    "CA certificate",
                    Input::new(&self.ca_location).w_full().into_any_element(),
                ))
            })
            .into_any_element()
    }

    pub fn build(&self, cx: &App) -> Result<(ConnectionProfile, Option<String>), String> {
        let name = self.name.read(cx).value().trim().to_string();
        let servers = self.servers.read(cx).value().trim().to_string();
        if servers.is_empty() {
            return Err("Bootstrap servers must not be empty".into());
        }
        let name = if name.is_empty() { servers.clone() } else { name };
        let (security, password) = if self.uses_sasl(cx) {
            let username = self.username.read(cx).value().trim().to_string();
            if username.is_empty() {
                return Err("Username must not be empty for SASL_SSL".into());
            }
            let mechanism_ix = self
                .mechanism
                .read(cx)
                .selected_index(cx)
                .map_or(0, |ix| ix.row);
            let mechanism = SaslMechanism::ALL[mechanism_ix.min(SaslMechanism::ALL.len() - 1)];
            let password = self.password.read(cx).value().to_string();
            (
                Security::SaslSsl {
                    mechanism,
                    username,
                },
                Some(password),
            )
        } else {
            (Security::Plaintext, None)
        };
        let ca_location = self.ca_location.read(cx).value().trim().to_string();
        let profile = ConnectionProfile {
            id: self.existing_id.clone().unwrap_or_else(new_profile_id),
            name,
            bootstrap_servers: servers,
            security,
            ca_location: (!ca_location.is_empty()).then_some(ca_location),
            password: password.clone(),
        };
        Ok((profile, password))
    }
}

pub fn open_connection_dialog(
    app: WeakEntity<KafkamitterApp>,
    existing: Option<ConnectionProfile>,
    password: Option<String>,
    window: &mut Window,
    cx: &mut App,
) {
    let form = Rc::new(ConnectionForm::new(
        existing.as_ref(),
        password.as_deref(),
        window,
        cx,
    ));
    let title = if existing.is_some() {
        "Edit connection"
    } else {
        "New connection"
    };
    window.open_dialog(cx, move |dialog, _window, cx| {
        let form_for_render = form.clone();
        let form_for_ok = form.clone();
        let app = app.clone();
        dialog
            .title(title)
            .w(px(520.))
            .child(form_for_render.render(cx))
            .footer(ok_cancel_footer("Save"))
            .on_ok(move |_, window, cx| match form_for_ok.build(cx) {
                Ok((profile, password)) => {
                    let name = profile.name.clone();
                    match app.update(cx, |app, cx| app.upsert_profile(profile, password, window, cx)) {
                        Ok(Ok(())) => {
                            window.push_notification(Notification::success(format!("Saved {name}")), cx);
                            true
                        }
                        Ok(Err(err)) => {
                            window.push_notification(
                                Notification::error(format!("Cannot save the connection: {err}")),
                                cx,
                            );
                            false
                        }
                        Err(_) => true,
                    }
                }
                Err(message) => {
                    window.push_notification(Notification::warning(message), cx);
                    false
                }
            })
    });
}
