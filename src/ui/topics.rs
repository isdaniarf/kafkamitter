use std::sync::Arc;

use gpui::*;
use gpui_component::input::{Input, InputState};
use gpui_component::list::{ListDelegate, ListItem, ListState};
use gpui_component::menu::ContextMenuExt;
use gpui_component::notification::Notification;
use gpui_component::{ActiveTheme, IndexPath, WindowExt, h_flex, v_flex};

use crate::app::KafkamitterApp;
use crate::kafka::admin::NewTopicSpec;
use crate::ui::dialog_footer::ok_cancel_footer;
use crate::kafka::metadata::TopicInfo;

actions!(topics, [CreateTopic, DeleteTopic]);

pub struct TopicListDelegate {
    all: Vec<Arc<TopicInfo>>,
    matched: Vec<usize>,
    query: String,
    show_internal: bool,
    selected: Option<IndexPath>,
    right_clicked: Option<IndexPath>,
}

impl TopicListDelegate {
    pub fn new() -> Self {
        Self {
            all: Vec::new(),
            matched: Vec::new(),
            query: String::new(),
            show_internal: false,
            selected: None,
            right_clicked: None,
        }
    }

    pub fn set_topics(&mut self, topics: Vec<Arc<TopicInfo>>) {
        self.all = topics;
        self.selected = None;
        self.filter();
    }

    pub fn set_show_internal(&mut self, show: bool) {
        self.show_internal = show;
        self.filter();
    }

    pub fn show_internal(&self) -> bool {
        self.show_internal
    }

    pub fn right_clicked_topic(&self) -> Option<Arc<TopicInfo>> {
        self.right_clicked.and_then(|ix| self.topic_at(ix))
    }

    pub fn selected_topic(&self) -> Option<Arc<TopicInfo>> {
        self.selected.and_then(|ix| self.topic_at(ix))
    }

    pub fn topic_at(&self, ix: IndexPath) -> Option<Arc<TopicInfo>> {
        self.matched.get(ix.row).and_then(|&i| self.all.get(i)).cloned()
    }

    pub fn select_topic(&mut self, name: &str) -> Option<IndexPath> {
        let row = self
            .matched
            .iter()
            .position(|&i| self.all[i].name == name)?;
        self.selected = Some(IndexPath::default().row(row));
        self.selected
    }

    fn filter(&mut self) {
        let query = self.query.as_str();
        self.matched = self
            .all
            .iter()
            .enumerate()
            .filter(|(_, t)| self.show_internal || !t.is_internal())
            .filter(|(_, t)| query.is_empty() || t.name.to_lowercase().contains(query))
            .map(|(i, _)| i)
            .collect();
    }
}

impl ListDelegate for TopicListDelegate {
    type Item = ListItem;

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.matched.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let topic = self.topic_at(ix)?;
        let selected = self.selected == Some(ix);
        Some(
            ListItem::new(("topic", ix.row))
                .selected(selected)
                .px_2()
                .py_1()
                .child(
                    h_flex()
                        .id(("topic-row", ix.row))
                        .w_full()
                        .text_sm()
                        .gap_2()
                        .justify_between()
                        .context_menu(|menu, _, _| menu.menu("Delete topic…", Box::new(DeleteTopic)))
                        .child(div().flex_1().min_w_0().truncate().child(topic.name.clone()))
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(format!("{}", topic.partitions.len())),
                        ),
                ),
        )
    }

    fn perform_search(
        &mut self,
        query: &str,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) -> Task<()> {
        self.query = query.trim().to_lowercase();
        self.filter();
        Task::ready(())
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) {
        self.selected = ix;
        cx.notify();
    }

    fn set_right_clicked_index(
        &mut self,
        ix: Option<IndexPath>,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) {
        self.right_clicked = ix;
    }
}

pub fn open_create_topic_dialog(app: WeakEntity<KafkamitterApp>, window: &mut Window, cx: &mut App) {
    let name = cx.new(|cx| InputState::new(window, cx).placeholder("topic name"));
    let partitions = cx.new(|cx| InputState::new(window, cx).default_value("1"));
    let replication = cx.new(|cx| InputState::new(window, cx).default_value("1"));
    let retention = cx.new(|cx| InputState::new(window, cx).placeholder("optional, milliseconds"));
    window.open_dialog(cx, move |dialog, _, cx| {
        let field = |label: &'static str, input: &Entity<InputState>| {
            v_flex()
                .gap_1()
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(label),
                )
                .child(Input::new(input).w_full())
        };
        let form = v_flex()
            .gap_3()
            .w_full()
            .child(field("Name", &name))
            .child(
                h_flex()
                    .gap_3()
                    .child(div().flex_1().child(field("Partitions", &partitions)))
                    .child(div().flex_1().child(field("Replication factor", &replication))),
            )
            .child(field("Retention (retention.ms)", &retention));
        let name = name.clone();
        let partitions = partitions.clone();
        let replication = replication.clone();
        let retention = retention.clone();
        let app = app.clone();
        dialog
            .title("Create topic")
            .w(px(480.))
            .child(form)
            .footer(ok_cancel_footer("Create"))
            .on_ok(move |_, window, cx| {
                let topic_name = name.read(cx).value().trim().to_string();
                if topic_name.is_empty() {
                    window.push_notification(Notification::warning("Topic name must not be empty"), cx);
                    return false;
                }
                let parse = |input: &Entity<InputState>, label: &str| -> Result<i32, String> {
                    input
                        .read(cx)
                        .value()
                        .trim()
                        .parse::<i32>()
                        .ok()
                        .filter(|n| *n >= 1)
                        .ok_or_else(|| format!("{label} must be a positive number"))
                };
                let spec = match (parse(&partitions, "Partitions"), parse(&replication, "Replication factor")) {
                    (Ok(partitions), Ok(replication_factor)) => {
                        let retention_value = retention.read(cx).value().trim().to_string();
                        let mut configs = Vec::new();
                        if !retention_value.is_empty() {
                            if retention_value.parse::<i64>().is_err() {
                                window.push_notification(Notification::warning("Retention must be a number of milliseconds"), cx);
                                return false;
                            }
                            configs.push(("retention.ms".to_string(), retention_value));
                        }
                        NewTopicSpec {
                            name: topic_name,
                            partitions,
                            replication_factor,
                            configs,
                        }
                    }
                    (Err(message), _) | (_, Err(message)) => {
                        window.push_notification(Notification::warning(message), cx);
                        return false;
                    }
                };
                let _ = app.update(cx, |app, cx| app.create_topic(spec, window, cx));
                true
            })
    });
}
