use std::sync::Arc;

use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Editor, EditorState};
use gpui_component::switch::Switch;
use gpui_component::tab::{Tab, TabBar};
use gpui_component::{ActiveTheme, IconName, Sizable, h_flex, v_flex};

use crate::model::json::{looks_like_json, try_pretty};
use crate::model::message::MessageRecord;

const INLINE_RENDER_BYTES: usize = 64 * 1024;
pub const LARGE_VALUE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DetailTab {
    Value,
    Key,
    Headers,
}

impl DetailTab {
    const ALL: [DetailTab; 3] = [DetailTab::Value, DetailTab::Key, DetailTab::Headers];

    fn label(self) -> &'static str {
        match self {
            DetailTab::Value => "Value",
            DetailTab::Key => "Key",
            DetailTab::Headers => "Headers",
        }
    }
}

pub struct MessageDetailView {
    record: Option<Arc<MessageRecord>>,
    tab: DetailTab,
    pretty: bool,
    editor: Entity<EditorState>,
    json: bool,
    generation: u64,
    rendering: bool,
}

impl MessageDetailView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editor = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
                .soft_wrap(true)
        });
        editor.update(cx, |editor, cx| {
            editor.set_highlighter_factory(crate::ui::json_highlight::factory(), cx);
            editor.set_highlighter("json", cx);
        });
        Self {
            record: None,
            tab: DetailTab::Value,
            pretty: true,
            editor,
            json: true,
            generation: 0,
            rendering: false,
        }
    }

    pub fn set_pretty_default(&mut self, pretty: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.pretty != pretty {
            self.pretty = pretty;
            self.refresh_text(window, cx);
        }
    }

    pub fn show_headers(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.tab = DetailTab::Headers;
        self.refresh_text(window, cx);
    }

    pub fn focus_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.record.is_none() {
            return;
        }
        self.editor.update(cx, |editor, cx| editor.focus(window, cx));
    }

    pub fn set_record(&mut self, record: Option<Arc<MessageRecord>>, window: &mut Window, cx: &mut Context<Self>) {
        self.record = record;
        self.refresh_text(window, cx);
    }

    fn refresh_text(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.generation += 1;
        let generation = self.generation;
        let Some(record) = self.record.clone() else {
            self.rendering = false;
            self.show(String::new(), false, window, cx);
            return;
        };
        let (tab, pretty) = (self.tab, self.pretty);
        if tab_bytes(&record, tab) <= INLINE_RENDER_BYTES {
            self.rendering = false;
            let (text, json) = render_tab(&record, tab, pretty);
            self.show(text, json, window, cx);
            return;
        }
        self.rendering = true;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let (text, json) = cx
                .background_spawn(async move { render_tab(&record, tab, pretty) })
                .await;
            let _ = this.update_in(cx, |view, window, cx| {
                if view.generation != generation {
                    return;
                }
                view.rendering = false;
                view.show(text, json, window, cx);
            });
        })
        .detach();
    }

    fn show(&mut self, text: String, json: bool, window: &mut Window, cx: &mut Context<Self>) {
        let changed = self.json != json;
        self.json = json;
        self.editor.update(cx, |editor, cx| {
            if changed {
                editor.set_highlighter(if json { "json" } else { "text" }, cx);
            }
            editor.set_value(text, window, cx);
        });
        cx.notify();
    }

    fn copy_text(&self, cx: &mut Context<Self>) {
        let text = self.editor.read(cx).value().to_string();
        cx.write_to_clipboard(ClipboardItem::new_string(text));
    }
}

fn tab_bytes(record: &MessageRecord, tab: DetailTab) -> usize {
    match tab {
        DetailTab::Value => record.value().map_or(0, <[u8]>::len),
        DetailTab::Key => record.key().map_or(0, <[u8]>::len),
        DetailTab::Headers => record
            .headers()
            .map(|(name, value)| name.len() + value.map_or(0, <[u8]>::len))
            .sum(),
    }
}

fn render_tab(record: &MessageRecord, tab: DetailTab, pretty: bool) -> (String, bool) {
    match tab {
        DetailTab::Value => render_bytes(record.value(), pretty),
        DetailTab::Key => render_bytes(record.key(), pretty),
        DetailTab::Headers => {
            if !record.has_headers() {
                return (String::from("<no headers>"), false);
            }
            let text = record
                .headers()
                .map(|(name, value)| {
                    format!(
                        "{name}: {}",
                        value.map_or("<null>".to_string(), |value| String::from_utf8_lossy(value).into_owned())
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            (text, false)
        }
    }
}

fn render_bytes(bytes: Option<&[u8]>, pretty: bool) -> (String, bool) {
    match bytes {
        None => (String::from("<null>"), false),
        Some([]) => (String::from("<empty>"), false),
        Some(bytes) if bytes.len() > LARGE_VALUE_BYTES => (String::from_utf8_lossy(bytes).into_owned(), false),
        Some(bytes) => {
            if pretty {
                if let Some(text) = try_pretty(bytes) {
                    return (text, true);
                }
            }
            (String::from_utf8_lossy(bytes).into_owned(), looks_like_json(bytes))
        }
    }
}

impl Render for MessageDetailView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(record) = self.record.clone() else {
            return v_flex()
                .size_full()
                .items_center()
                .justify_center()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child("Select a message to inspect it")
                .into_any_element();
        };
        let tab_ix = DetailTab::ALL.iter().position(|t| *t == self.tab).unwrap_or(0);
        let value_bytes = record.value().map_or(0, <[u8]>::len);
        let mut meta = format!(
            "partition {}  offset {}  {}  {} bytes",
            record.partition,
            record.offset,
            record.timestamp_text(),
            value_bytes
        );
        if self.rendering {
            meta.push_str("  ·  rendering");
        } else if self.tab == DetailTab::Value && value_bytes > LARGE_VALUE_BYTES {
            meta.push_str("  ·  large value, shown as plain text");
        }
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .w_full()
                    .px_2()
                    .gap_3()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        TabBar::new("detail-tabs")
                            .underline()
                            .selected_index(tab_ix)
                            .on_click(cx.listener(|this, ix: &usize, window, cx| {
                                this.tab = DetailTab::ALL[(*ix).min(DetailTab::ALL.len() - 1)];
                                this.refresh_text(window, cx);
                            }))
                            .children(DetailTab::ALL.iter().map(|t| Tab::new().label(t.label()))),
                    )
                    .child(
                        Switch::new("pretty-json")
                            .small()
                            .checked(self.pretty)
                            .label("Pretty JSON")
                            .on_change(cx.listener(|this, checked: &bool, window, cx| {
                                this.pretty = *checked;
                                this.refresh_text(window, cx);
                            })),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(meta),
                    )
                    .child(
                        Button::new("copy-detail")
                            .ghost()
                            .xsmall()
                            .icon(IconName::Copy)
                            .tooltip("Copy")
                            .on_click(cx.listener(|this, _, _, cx| this.copy_text(cx))),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .child(Editor::new(&self.editor).readonly(true).h(relative(1.))),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{LARGE_VALUE_BYTES, render_bytes};

    #[test]
    fn json_values_ask_for_the_json_highlighter() {
        let (text, json) = render_bytes(Some(br#"{"id":1}"#), true);
        assert_eq!(text, "{\n  \"id\": 1\n}");
        assert!(json);
    }

    #[test]
    fn plain_values_ask_for_no_highlighter() {
        let (text, json) = render_bytes(Some(b"27a8bf96-332e-4644"), true);
        assert_eq!(text, "27a8bf96-332e-4644");
        assert!(!json, "a header value must not use the json highlighter");
    }

    #[test]
    fn a_missing_or_empty_value_asks_for_no_highlighter() {
        assert_eq!(render_bytes(None, true), (String::from("<null>"), false));
        assert_eq!(render_bytes(Some(&[]), true), (String::from("<empty>"), false));
    }

    #[test]
    fn raw_json_keeps_the_json_highlighter() {
        let (text, json) = render_bytes(Some(br#"{"id":1}"#), false);
        assert_eq!(text, r#"{"id":1}"#);
        assert!(json);
    }

    #[test]
    fn a_large_value_is_shown_raw_without_a_highlighter() {
        let big = format!("{{\"a\":\"{}\"}}", "x".repeat(LARGE_VALUE_BYTES));
        let (text, json) = render_bytes(Some(big.as_bytes()), true);
        assert_eq!(text, big);
        assert!(!json);
    }
}
