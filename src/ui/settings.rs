use std::cell::Cell;
use std::rc::Rc;

use gpui::*;
use gpui_component::input::{Input, InputState};
use gpui_component::notification::Notification;
use gpui_component::switch::Switch;
use gpui_component::{ActiveTheme, WindowExt, h_flex, v_flex};

use crate::app::KafkamitterApp;
use crate::model::settings::Settings;
use crate::ui::dialog_footer::ok_cancel_footer;

fn switch_row(id: &'static str, label: &'static str, value: &Rc<Cell<bool>>) -> AnyElement {
    let cell = value.clone();
    h_flex()
        .w_full()
        .justify_between()
        .items_center()
        .child(div().text_sm().child(label))
        .child(
            Switch::new(id)
                .checked(value.get())
                .on_change(move |checked, window, _| {
                    cell.set(*checked);
                    window.refresh();
                }),
        )
        .into_any_element()
}

pub fn open_settings_dialog(app: WeakEntity<KafkamitterApp>, current: Settings, window: &mut Window, cx: &mut App) {
    let newest = cx.new(|cx| InputState::new(window, cx).default_value(current.newest_per_partition.to_string()));
    let max_messages = cx.new(|cx| InputState::new(window, cx).default_value(current.max_messages.to_string()));
    let max_megabytes = cx.new(|cx| InputState::new(window, cx).default_value(current.max_megabytes.to_string()));
    let auto_consume = Rc::new(Cell::new(current.auto_consume_on_select));
    let open_newest = Rc::new(Cell::new(current.open_newest_message));
    let pretty_json = Rc::new(Cell::new(current.pretty_json_default));
    window.open_dialog(cx, move |dialog, _, cx| {
        let field = |label: &'static str, input: &Entity<InputState>| {
            h_flex()
                .w_full()
                .justify_between()
                .items_center()
                .gap_3()
                .child(div().text_sm().child(label))
                .child(Input::new(input).w(px(140.)))
        };
        let form = v_flex()
            .gap_3()
            .w_full()
            .child(switch_row("auto-consume", "Consume when a topic is selected", &auto_consume))
            .child(switch_row("open-newest", "Open the newest message after loading", &open_newest))
            .child(switch_row("pretty-json", "Show values as pretty JSON", &pretty_json))
            .child(div().border_t_1().border_color(cx.theme().border))
            .child(field("Newest messages per partition", &newest))
            .child(field("Maximum messages kept", &max_messages))
            .child(field("Maximum memory for messages (MB)", &max_megabytes));
        let newest = newest.clone();
        let max_messages = max_messages.clone();
        let max_megabytes = max_megabytes.clone();
        let auto_consume = auto_consume.clone();
        let open_newest = open_newest.clone();
        let pretty_json = pretty_json.clone();
        let app = app.clone();
        dialog
            .title("Settings")
            .w(px(480.))
            .child(form)
            .footer(ok_cancel_footer("Save"))
            .on_ok(move |_, window, cx| {
                let parse = |input: &Entity<InputState>, label: &str| -> Result<usize, String> {
                    input
                        .read(cx)
                        .value()
                        .trim()
                        .parse::<usize>()
                        .ok()
                        .filter(|n| *n >= 1)
                        .ok_or_else(|| format!("{label} must be a positive number"))
                };
                let parsed = (
                    parse(&newest, "Newest messages per partition"),
                    parse(&max_messages, "Maximum messages kept"),
                    parse(&max_megabytes, "Maximum memory"),
                );
                let settings = match parsed {
                    (Ok(newest), Ok(max_messages), Ok(max_megabytes)) => Settings {
                        auto_consume_on_select: auto_consume.get(),
                        newest_per_partition: newest as i64,
                        open_newest_message: open_newest.get(),
                        pretty_json_default: pretty_json.get(),
                        max_messages,
                        max_megabytes,
                    },
                    (Err(message), _, _) | (_, Err(message), _) | (_, _, Err(message)) => {
                        window.push_notification(Notification::warning(message), cx);
                        return false;
                    }
                };
                let _ = app.update(cx, |app, cx| app.apply_settings(settings, window, cx));
                true
            })
    });
}
