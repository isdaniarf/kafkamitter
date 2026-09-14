use std::cell::Cell;
use std::rc::Rc;

use gpui::*;
use gpui_component::input::{Input, InputState};
use gpui_component::select::{Select, SelectEvent, SelectState};
use gpui_component::notification::Notification;
use gpui_component::switch::Switch;
use gpui_component::{ActiveTheme, WindowExt, h_flex, v_flex};

use crate::app::KafkamitterApp;
use crate::model::settings::{Settings, ThemeChoice};
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

/// Opens the settings dialog and returns the subscription that keeps the
/// appearance dropdown live. The caller must hold it while the dialog is open.
pub fn open_settings_dialog(
    app: WeakEntity<KafkamitterApp>,
    current: Settings,
    window: &mut Window,
    cx: &mut Context<KafkamitterApp>,
) -> Subscription {
    let newest = cx.new(|cx| InputState::new(window, cx).default_value(current.newest_per_partition.to_string()));
    let max_messages = cx.new(|cx| InputState::new(window, cx).default_value(current.max_messages.to_string()));
    let max_megabytes = cx.new(|cx| InputState::new(window, cx).default_value(current.max_megabytes.to_string()));
    let theme_ix = ThemeChoice::ALL
        .iter()
        .position(|choice| *choice == current.theme)
        .unwrap_or(0);
    let theme = cx.new(|cx| {
        SelectState::new(
            ThemeChoice::ALL
                .iter()
                .map(|choice| SharedString::from(choice.label()))
                .collect::<Vec<_>>(),
            Some(gpui_component::IndexPath::default().row(theme_ix)),
            window,
            cx,
        )
    });
    // `cx` already belongs to the app, so subscribe through it. Reaching for the
    // entity here would update it while it is being updated, which panics.
    let theme_preview = cx.subscribe_in(
        &theme,
        window,
        |_, select, _: &SelectEvent<Vec<SharedString>>, window, cx| {
            let row = select.read(cx).selected_index(cx).map_or(0, |ix| ix.row);
            let choice = ThemeChoice::ALL[row.min(ThemeChoice::ALL.len() - 1)];
            KafkamitterApp::preview_theme(choice, window, cx);
        },
    );
    let saved = Rc::new(Cell::new(false));
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
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .items_center()
                    .gap_3()
                    .child(div().text_sm().child("Appearance"))
                    .child(Select::new(&theme).w(px(200.))),
            )
            .child(div().border_t_1().border_color(cx.theme().border))
            .child(switch_row("auto-consume", "Consume when a topic is selected", &auto_consume))
            .child(switch_row("open-newest", "Open the newest message after loading", &open_newest))
            .child(switch_row("pretty-json", "Show values as pretty JSON", &pretty_json))
            .child(div().border_t_1().border_color(cx.theme().border))
            .child(field("Newest messages per partition", &newest))
            .child(field("Maximum messages kept", &max_messages))
            .child(field("Maximum memory for messages (MB)", &max_megabytes));
        let theme = theme.clone();
        let newest = newest.clone();
        let max_messages = max_messages.clone();
        let max_megabytes = max_megabytes.clone();
        let auto_consume = auto_consume.clone();
        let open_newest = open_newest.clone();
        let pretty_json = pretty_json.clone();
        let app = app.clone();
        let saved_on_ok = saved.clone();
        let saved_on_close = saved.clone();
        let app_on_close = app.clone();
        dialog
            .title("Settings")
            .w(px(480.))
            .child(form)
            .footer(ok_cancel_footer("Save"))
            .on_close(move |_, window, cx| {
                // Closing without saving puts the appearance back.
                if !saved_on_close.get() {
                    let _ = app_on_close.update(cx, |this, cx| this.apply_theme(window, cx));
                }
            })
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
                let theme_choice = theme
                    .read(cx)
                    .selected_index(cx)
                    .map_or(ThemeChoice::System, |ix| {
                        ThemeChoice::ALL[ix.row.min(ThemeChoice::ALL.len() - 1)]
                    });
                let settings = match parsed {
                    (Ok(newest), Ok(max_messages), Ok(max_megabytes)) => Settings {
                        theme: theme_choice,
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
                saved_on_ok.set(true);
                let _ = app.update(cx, |app, cx| app.apply_settings(settings, window, cx));
                true
            })
    });
    theme_preview
}
