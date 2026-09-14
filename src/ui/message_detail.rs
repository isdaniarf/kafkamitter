use std::sync::Arc;

use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Editor, EditorState};
use gpui_component::switch::Switch;
use gpui_component::tab::{Tab, TabBar};
use gpui_component::{ActiveTheme, IconName, Sizable, h_flex, v_flex};

use crate::model::json::{looks_like_json, try_pretty};
use crate::model::message::MessageRecord;
use crate::ui::messages::format_timestamp;

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
    text: String,
    json: bool,
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
            text: String::new(),
            json: true,
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
        let (text, json) = match &self.record {
            None => (String::new(), false),
            Some(record) => match self.tab {
                DetailTab::Value => render_bytes(record.value.as_deref(), self.pretty),
                DetailTab::Key => render_bytes(record.key.as_deref(), self.pretty),
                DetailTab::Headers => {
                    if record.headers.is_empty() {
                        (String::from("<no headers>"), false)
                    } else {
                        let text = record
                            .headers
                            .iter()
                            .map(|(k, v)| {
                                format!(
                                    "{k}: {}",
                                    v.as_deref().map_or("<null>".to_string(), |v| String::from_utf8_lossy(v).into_owned())
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        (text, false)
                    }
                }
            },
        };
        self.text = text.clone();
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
        cx.write_to_clipboard(ClipboardItem::new_string(self.text.clone()));
    }
}

fn render_bytes(bytes: Option<&[u8]>, pretty: bool) -> (String, bool) {
    match bytes {
        None => (String::from("<null>"), false),
        Some([]) => (String::from("<empty>"), false),
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
        let meta = format!(
            "partition {}  offset {}  {}  {} bytes",
            record.partition,
            record.offset,
            format_timestamp(record.timestamp_ms),
            record.value.as_ref().map_or(0, Vec::len)
        );
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
    use super::render_bytes;

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
}
