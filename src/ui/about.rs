use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::dialog::{DialogClose, DialogFooter};
use gpui_component::link::Link;
use gpui_component::{ActiveTheme, StyledExt, WindowExt, h_flex, v_flex};

use crate::app::KafkamitterApp;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const HOMEPAGE: &str = "https://github.com/isdaniarf/kafkamitter";

pub fn librdkafka_version() -> String {
    rdkafka::util::get_rdkafka_version().1
}

pub fn summary() -> String {
    format!(
        "Kafkamitter {VERSION} ({} {}, librdkafka {})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        librdkafka_version()
    )
}

pub fn open_about_dialog(window: &mut Window, cx: &mut Context<KafkamitterApp>) {
    window.open_dialog(cx, move |dialog, _, cx| {
        let row = |label: &'static str, value: String| {
            h_flex()
                .w_full()
                .justify_between()
                .items_center()
                .gap_3()
                .child(div().text_sm().text_color(cx.theme().muted_foreground).child(label))
                .child(div().text_sm().child(value))
        };
        let body = v_flex()
            .gap_2()
            .w_full()
            .child(div().text_lg().font_semibold().child("Kafkamitter"))
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("A native Kafka client for macOS"),
            )
            .child(div().h_2())
            .child(row("Version", VERSION.to_string()))
            .child(row(
                "Platform",
                format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            ))
            .child(row("librdkafka", librdkafka_version()))
            .child(div().h_2())
            .child(
                Link::new("about-homepage")
                    .href(HOMEPAGE)
                    .text_xs()
                    .child(HOMEPAGE),
            );
        dialog
            .w(px(420.))
            .child(body)
            .footer(
                DialogFooter::new()
                    .child(
                        div().flex_none().child(
                            Button::new("about-copy")
                                .outline()
                                .label("Copy version")
                                .on_click(|_, _, cx: &mut App| {
                                    cx.write_to_clipboard(ClipboardItem::new_string(summary()));
                                }),
                        ),
                    )
                    .child(
                        div()
                            .flex_none()
                            .child(DialogClose::new().child(Button::new("about-close").primary().label("Close"))),
                    ),
            )
    });
}

#[cfg(test)]
mod tests {
    use super::{VERSION, librdkafka_version, summary};

    #[test]
    fn the_summary_holds_the_app_version() {
        let text = summary();
        assert!(text.starts_with("Kafkamitter "), "got {text}");
        assert!(text.contains(VERSION), "got {text}");
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn the_summary_holds_the_client_library_version() {
        let version = librdkafka_version();
        assert!(version.starts_with(|c: char| c.is_ascii_digit()), "got {version}");
        assert!(summary().contains(&version));
    }
}
