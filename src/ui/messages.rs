use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;

use gpui::prelude::FluentBuilder as _;
use gpui_base::{SelectableText, TextSelectionHandle};
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::notification::Notification;
use gpui_component::resizable::{ResizableState, resizable_panel, v_resizable};
use gpui_component::select::{Select, SelectState};
use gpui_component::table::{Column, ColumnSort, DataTable, TableDelegate, TableEvent, TableState};
use gpui_component::{ActiveTheme, Icon, IconName, IndexPath, Size, Sizable, WindowExt, h_flex, v_flex};

use crate::kafka::consume::{ConsumeEvent, ConsumeSession, StartFrom};
use crate::kafka::metadata::TopicInfo;
use crate::kafka::worker::WorkerHandle;
use crate::model::message::{MessageRecord, MessageStore};
use crate::model::search;
use crate::model::settings::Settings;
use crate::ui::message_detail::MessageDetailView;

const MAX_MESSAGES: usize = 10_000;
const MAX_BYTES: usize = 256 * 1024 * 1024;
const NEWEST_PER_PARTITION: i64 = 200;
const START_MODES: [&str; 5] = ["Newest 200", "Latest", "Beginning", "Offset", "Timestamp"];

pub fn format_timestamp(timestamp_ms: Option<i64>) -> String {
    match timestamp_ms.and_then(chrono::DateTime::from_timestamp_millis) {
        Some(utc) => utc
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        None => String::from("-"),
    }
}

pub struct MessageTableDelegate {
    pub store: MessageStore,
    columns: Vec<Column>,
    sort: Option<(SharedString, ColumnSort)>,
    /// One selection document for the whole table, so a drag can cross cells.
    selection: Option<TextSelectionHandle>,
    /// Display row to store row, after the search term and the sort apply.
    rows: Vec<usize>,
    rows_dirty: bool,
}

impl MessageTableDelegate {
    fn new() -> Self {
        Self {
            store: MessageStore::new(MAX_MESSAGES, MAX_BYTES),
            columns: vec![
                Column::new("partition", "Part.").width(px(64.)).text_right().sortable(),
                Column::new("offset", "Offset").width(px(110.)).text_right().sortable(),
                Column::new("timestamp", "Timestamp").width(px(240.)).sortable(),
                Column::new("key", "Key").width(px(220.)).sortable(),
                Column::new("value", "Value").width(px(900.)).sortable(),
            ],
            sort: None,
            selection: None,
            rows: Vec::new(),
            rows_dirty: true,
        }
    }

    pub fn push_batch(&mut self, batch: Vec<MessageRecord>) {
        self.store.push_batch(batch);
        self.rows_dirty = true;
    }

    pub fn clear(&mut self) {
        self.store.clear();
        self.rows.clear();
        self.rows_dirty = true;
    }

    pub fn set_query(&mut self, query: &str) {
        self.store.set_query(search::needle(query));
        self.rows_dirty = true;
    }

    pub fn mark_dirty(&mut self) {
        self.rows_dirty = true;
    }

    /// Rebuilds the visible rows. It runs at most once for each frame.
    pub fn ensure_rows(&mut self) {
        if !self.rows_dirty {
            return;
        }
        self.rows_dirty = false;
        let mut rows = self.store.matched_rows();
        if let Some((key, direction)) = self.sort.clone() {
            let store = &self.store;
            rows.sort_by(|&a, &b| {
                let (Some(left), Some(right)) = (store.get(a), store.get(b)) else {
                    return std::cmp::Ordering::Equal;
                };
                match key.as_ref() {
                    "partition" => left.partition.cmp(&right.partition).then(left.offset.cmp(&right.offset)),
                    "offset" => left.offset.cmp(&right.offset).then(left.partition.cmp(&right.partition)),
                    "timestamp" => left.timestamp_ms.cmp(&right.timestamp_ms).then(left.offset.cmp(&right.offset)),
                    "key" => left.key_preview.cmp(&right.key_preview),
                    "value" => left.value_preview.cmp(&right.value_preview),
                    _ => std::cmp::Ordering::Equal,
                }
            });
            if direction == ColumnSort::Descending {
                rows.reverse();
            }
        }
        self.rows = rows;
    }

    pub fn record_at_row(&self, row: usize) -> Option<&Arc<MessageRecord>> {
        self.store.get(*self.rows.get(row)?)
    }

    /// The visible row that holds the newest or the oldest message.
    pub fn extreme_row(&self, newest: bool) -> Option<usize> {
        let mut best: Option<(usize, (Option<i64>, i64))> = None;
        for (row, &store_row) in self.rows.iter().enumerate() {
            let Some(record) = self.store.get(store_row) else {
                continue;
            };
            let key = (record.timestamp_ms, record.offset);
            let better = best.is_none_or(|(_, current)| if newest { key > current } else { key < current });
            if better {
                best = Some((row, key));
            }
        }
        best.map(|(row, _)| row)
    }

    fn text_for(&self, record: &MessageRecord, col_ix: usize) -> String {
        match self.columns.get(col_ix).map(|c| c.key.as_ref()) {
            Some("partition") => record.partition.to_string(),
            Some("offset") => record.offset.to_string(),
            Some("timestamp") => format_timestamp(record.timestamp_ms),
            Some("key") => record.key_preview.clone(),
            Some("value") => record.value_preview.clone(),
            _ => String::new(),
        }
    }
}

impl TableDelegate for MessageTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        if self.store.has_query() {
            self.store.matched_len()
        } else {
            self.store.len()
        }
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns[col_ix].clone()
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        self.ensure_rows();
        let Some(text) = self
            .record_at_row(row_ix)
            .map(|record| self.text_for(record, col_ix))
        else {
            return div().into_any_element();
        };
        let muted = matches!(col_ix, 0 | 2);
        let order = row_ix * self.columns.len() + col_ix;
        let selection = self
            .selection
            .get_or_insert_with(|| TextSelectionHandle::new("", cx))
            .clone();
        div()
            .px_2()
            .text_size(cx.theme().mono_font_size)
            .truncate()
            .when(col_ix >= 3, |el| el.font_family(cx.theme().mono_font_family.clone()))
            .when(muted, |el| el.text_color(cx.theme().muted_foreground))
            .child(
                SelectableText::with_handle(("cell", order), selection, text)
                    .document_order(order as u64),
            )
            .into_any_element()
    }

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        self.record_at_row(row_ix)
            .map(|record| self.text_for(record, col_ix))
            .unwrap_or_default()
    }

    fn perform_sort(&mut self, col_ix: usize, sort: ColumnSort, _window: &mut Window, _cx: &mut Context<TableState<Self>>) {
        self.sort = match (self.columns.get(col_ix), sort) {
            (_, ColumnSort::Default) | (None, _) => None,
            (Some(column), sort) => Some((column.key.clone(), sort)),
        };
        self.rows_dirty = true;
    }

    fn move_column(&mut self, col_ix: usize, to_ix: usize, _window: &mut Window, _cx: &mut Context<TableState<Self>>) {
        if col_ix < self.columns.len() && to_ix < self.columns.len() {
            let column = self.columns.remove(col_ix);
            self.columns.insert(to_ix, column);
        }
    }
}

pub struct MessagesView {
    worker: Option<Rc<WorkerHandle>>,
    topic: Option<Arc<TopicInfo>>,
    table: Entity<TableState<MessageTableDelegate>>,
    start_mode: Entity<SelectState<Vec<SharedString>>>,
    start_value: Entity<InputState>,
    partition: Entity<SelectState<Vec<SharedString>>>,
    search: Entity<InputState>,
    detail: Entity<MessageDetailView>,
    split: Entity<ResizableState>,
    settings: Settings,
    open_newest_pending: bool,
    session: Option<ConsumeSession>,
    generation: u64,
    placeholder_mode: usize,
    running: bool,
    assigned: Vec<i32>,
    eof: BTreeSet<i32>,
    last_error: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl MessagesView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let table = cx.new(|cx| {
            TableState::new(MessageTableDelegate::new(), window, cx)
                .row_selectable(true)
                .col_resizable(true)
                .col_movable(true)
                .sortable(true)
        });
        let start_mode = cx.new(|cx| {
            SelectState::new(
                START_MODES.iter().map(|s| SharedString::from(*s)).collect::<Vec<_>>(),
                Some(IndexPath::default()),
                window,
                cx,
            )
        });
        let start_value = cx.new(|cx| InputState::new(window, cx).placeholder("offset"));
        let partition = cx.new(|cx| {
            SelectState::new(vec![SharedString::from("All partitions")], Some(IndexPath::default()), window, cx)
        });
        let search = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search key, value, headers")
                .clean_on_escape()
        });
        let detail = cx.new(|cx| MessageDetailView::new(window, cx));
        let split = cx.new(|_| ResizableState::default());
        let subscriptions = vec![
            cx.subscribe_in(&table, window, Self::on_table_event),
            cx.subscribe_in(&search, window, Self::on_search_event),
        ];
        Self {
            worker: None,
            topic: None,
            table,
            start_mode,
            start_value,
            partition,
            search,
            detail,
            split,
            settings: Settings::default(),
            open_newest_pending: false,
            session: None,
            generation: 0,
            placeholder_mode: 0,
            running: false,
            assigned: Vec::new(),
            eof: BTreeSet::new(),
            last_error: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn apply_settings(&mut self, settings: &Settings, window: &mut Window, cx: &mut Context<Self>) {
        self.settings = settings.clone();
        self.table.update(cx, |table, cx| {
            table.delegate_mut().store.set_limits(settings.max_messages, settings.max_bytes());
            table.delegate_mut().mark_dirty();
            table.refresh(cx);
        });
        self.detail.update(cx, |detail, cx| detail.set_pretty_default(settings.pretty_json_default, window, cx));
        cx.notify();
    }

    pub fn start_default(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let newest = self.settings.newest_per_partition.max(1);
        self.start_with(StartFrom::Newest(newest), Vec::new(), window, cx);
    }

    fn jump_to(&mut self, newest: bool, window: &mut Window, cx: &mut Context<Self>) {
        let record = self.table.update(cx, |table, cx| {
            table.delegate_mut().ensure_rows();
            let row = table.delegate().extreme_row(newest)?;
            let record = table.delegate().record_at_row(row).cloned();
            table.set_selected_row(row, cx);
            table.scroll_to_row(row, cx);
            record
        });
        if let Some(record) = record {
            self.detail.update(cx, |detail, cx| detail.set_record(Some(record), window, cx));
        }
        cx.notify();
    }

    pub fn go_to_newest(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.jump_to(true, window, cx);
    }

    pub fn go_to_oldest(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.jump_to(false, window, cx);
    }

    pub fn set_search(&mut self, query: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.search.update(cx, |input, cx| input.set_value(query, window, cx));
        self.table.update(cx, |table, cx| {
            table.delegate_mut().set_query(query);
            table.clear_selection(cx);
            table.refresh(cx);
        });
        cx.notify();
    }

    pub fn focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.search.update(cx, |input, cx| input.focus(window, cx));
    }

    fn on_search_event(
        &mut self,
        search: &Entity<InputState>,
        event: &InputEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !matches!(event, InputEvent::Change) {
            return;
        }
        let query = search.read(cx).value().to_string();
        self.table.update(cx, |table, cx| {
            table.delegate_mut().set_query(&query);
            table.clear_selection(cx);
            table.refresh(cx);
        });
        cx.notify();
    }

    pub fn focus_value(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.detail.update(cx, |detail, cx| detail.focus_editor(window, cx));
    }

    pub fn set_topic(
        &mut self,
        worker: Option<Rc<WorkerHandle>>,
        topic: Option<Arc<TopicInfo>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let same_topic = match (&self.topic, &topic) {
            (Some(a), Some(b)) => a.name == b.name && self.worker.as_ref().map(Rc::as_ptr) == worker.as_ref().map(Rc::as_ptr),
            (None, None) => true,
            _ => false,
        };
        if same_topic {
            self.worker = worker;
            self.topic = topic;
            return false;
        }
        self.stop(cx);
        self.clear(window, cx);
        self.worker = worker;
        self.topic = topic;
        let mut items = vec![SharedString::from("All partitions")];
        if let Some(topic) = &self.topic {
            items.extend(topic.partitions.iter().map(|p| SharedString::from(format!("Partition {}", p.id))));
        }
        self.partition.update(cx, |select, cx| {
            select.set_items(items, window, cx);
            select.set_selected_index(Some(IndexPath::default()), window, cx);
        });
        cx.notify();
        true
    }

    fn selected_partitions(&self, cx: &App) -> Vec<i32> {
        let ix = self.partition.read(cx).selected_index(cx).map_or(0, |ix| ix.row);
        match (ix, &self.topic) {
            (0, _) | (_, None) => Vec::new(),
            (ix, Some(topic)) => topic.partitions.get(ix - 1).map(|p| vec![p.id]).unwrap_or_default(),
        }
    }

    fn start_mode_ix(&self, cx: &App) -> usize {
        self.start_mode.read(cx).selected_index(cx).map_or(0, |ix| ix.row)
    }

    fn parse_start(&self, cx: &App) -> Result<StartFrom, String> {
        let value = self.start_value.read(cx).value().trim().to_string();
        match self.start_mode_ix(cx) {
            0 => Ok(StartFrom::Newest(NEWEST_PER_PARTITION)),
            1 => Ok(StartFrom::Latest),
            2 => Ok(StartFrom::Beginning),
            3 => value
                .parse::<i64>()
                .map(StartFrom::Offset)
                .map_err(|_| "Enter a numeric offset".to_string()),
            _ => parse_timestamp(&value).map(StartFrom::Timestamp),
        }
    }

    fn start(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.worker.is_none() || self.topic.is_none() {
            return;
        }
        let start = match self.parse_start(cx) {
            Ok(start) => start,
            Err(message) => {
                window.push_notification(Notification::warning(message), cx);
                return;
            }
        };
        let partitions = self.selected_partitions(cx);
        self.start_with(start, partitions, window, cx);
    }

    pub fn start_with(&mut self, start: StartFrom, partitions: Vec<i32>, window: &mut Window, cx: &mut Context<Self>) {
        let (Some(worker), Some(topic)) = (self.worker.clone(), self.topic.clone()) else {
            return;
        };
        self.stop(cx);
        self.generation += 1;
        let generation = self.generation;
        let (tx, rx) = smol::channel::bounded::<ConsumeEvent>(64);
        self.session = Some(ConsumeSession::start(
            worker.base_config(),
            topic.name.clone(),
            partitions,
            start,
            tx,
        ));
        self.running = true;
        self.assigned.clear();
        self.eof.clear();
        self.last_error = None;
        self.open_newest_pending = self.settings.open_newest_message;
        cx.spawn_in(window, async move |this, cx| {
            while let Ok(event) = rx.recv().await {
                if this
                    .update_in(cx, |view, window, cx| view.on_event(generation, event, window, cx))
                    .is_err()
                {
                    break;
                }
            }
            let _ = this.update_in(cx, |view, _, cx| {
                if view.generation == generation {
                    view.running = false;
                    cx.notify();
                }
            });
        })
        .detach();
        cx.notify();
    }

    pub fn stop(&mut self, cx: &mut Context<Self>) {
        if let Some(session) = self.session.take() {
            session.stop();
        }
        self.running = false;
        cx.notify();
    }

    fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.table.update(cx, |table, cx| {
            table.delegate_mut().clear();
            table.refresh(cx);
        });
        self.detail.update(cx, |detail, cx| detail.set_record(None, window, cx));
        self.eof.clear();
        self.last_error = None;
        cx.notify();
    }

    fn on_event(&mut self, generation: u64, event: ConsumeEvent, window: &mut Window, cx: &mut Context<Self>) {
        if generation != self.generation {
            return;
        }
        match event {
            ConsumeEvent::Assigned(partitions) => {
                crate::startup::trace(&format!("consume assigned partitions {partitions:?}"));
                self.assigned = partitions;
            }
            ConsumeEvent::Batch(batch) => self.table.update(cx, |table, cx| {
                table.delegate_mut().push_batch(batch);
                table.refresh(cx);
                crate::startup::trace(&format!(
                    "consume batch: {} shown, {} received",
                    table.delegate().store.len(),
                    table.delegate().store.total_received()
                ));
            }),
            ConsumeEvent::Eof(partition) => {
                crate::startup::trace(&format!("consume eof partition {partition}"));
                self.eof.insert(partition);
                if !self.assigned.is_empty() && self.eof.len() >= self.assigned.len() {
                    let store = &self.table.read(cx).delegate().store;
                    crate::startup::trace(&format!(
                        "search: {} of {} rows match",
                        store.matched_len(),
                        store.len()
                    ));
                }
                if self.open_newest_pending && !self.assigned.is_empty() && self.eof.len() >= self.assigned.len() {
                    self.open_newest_pending = false;
                    self.go_to_newest(window, cx);
                    crate::startup::trace("opened newest message");
                }
            }
            ConsumeEvent::Error(message) => {
                crate::startup::trace(&format!("consume error: {message}"));
                self.last_error = Some(message);
            }
        }
        cx.notify();
    }

    fn on_table_event(
        &mut self,
        table: &Entity<TableState<MessageTableDelegate>>,
        event: &TableEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let TableEvent::SelectRow(row_ix) | TableEvent::DoubleClickedRow(row_ix) = event {
            let record = table.read(cx).delegate().record_at_row(*row_ix).cloned();
            self.detail.update(cx, |detail, cx| detail.set_record(record, window, cx));
        }
    }

    fn status_text(&self, cx: &App) -> String {
        let store = &self.table.read(cx).delegate().store;
        let mut parts = if store.has_query() {
            vec![format!("{} of {} shown", store.matched_len(), store.len())]
        } else {
            vec![format!("{} shown", store.len())]
        };
        if store.total_received() as usize > store.len() {
            parts.push(format!("{} received", store.total_received()));
        }
        parts.push(format_bytes(store.total_bytes()));
        if self.running && !self.assigned.is_empty() {
            parts.push(format!("caught up {}/{}", self.eof.len(), self.assigned.len()));
        }
        if let Some(err) = &self.last_error {
            parts.push(format!("error: {err}"));
        }
        parts.join("  ·  ")
    }
}

fn format_bytes(bytes: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn parse_timestamp(value: &str) -> Result<i64, String> {
    if value.is_empty() {
        return Err("Enter a timestamp as unix milliseconds or YYYY-MM-DD HH:MM:SS".into());
    }
    if let Ok(ms) = value.parse::<i64>() {
        return Ok(ms);
    }
    let formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"];
    for format in formats {
        let parsed = if format == "%Y-%m-%d" {
            chrono::NaiveDate::parse_from_str(value, format).map(|d| d.and_hms_opt(0, 0, 0).unwrap_or_default())
        } else {
            chrono::NaiveDateTime::parse_from_str(value, format)
        };
        if let Ok(naive) = parsed {
            if let Some(local) = naive.and_local_timezone(chrono::Local).single() {
                return Ok(local.timestamp_millis());
            }
        }
    }
    Err("Timestamp must be unix milliseconds or YYYY-MM-DD HH:MM:SS".into())
}

impl Render for MessagesView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mode = self.start_mode_ix(cx);
        let needs_value = mode >= 3;
        if needs_value && self.placeholder_mode != mode {
            self.placeholder_mode = mode;
            let placeholder = if mode == 3 { "offset" } else { "unix ms or YYYY-MM-DD HH:MM:SS" };
            self.start_value.update(cx, |input, cx| input.set_placeholder(placeholder, window, cx));
        }
        let status = self.status_text(cx);
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .w_full()
                    .px_3()
                    .py_2()
                    .gap_2()
                    .items_center()
                    .flex_nowrap()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .flex_none()
                            .child(Select::new(&self.start_mode).w(px(140.)).small()),
                    )
                    .when(needs_value, |el| {
                        el.child(
                            div()
                                .flex_none()
                                .child(Input::new(&self.start_value).w(px(200.)).small()),
                        )
                    })
                    .child(
                        div()
                            .flex_none()
                            .child(Select::new(&self.partition).w(px(150.)).small()),
                    )
                    .child(div().flex_none().child(if self.running {
                        Button::new("stop")
                            .danger()
                            .small()
                            .icon(IconName::Pause)
                            .label("Stop")
                            .on_click(cx.listener(|this, _, _, cx| this.stop(cx)))
                    } else {
                        Button::new("start")
                            .primary()
                            .small()
                            .icon(IconName::Play)
                            .label("Consume")
                            .on_click(cx.listener(|this, _, window, cx| this.start(window, cx)))
                    }))
                    .child(
                        div().flex_none().child(
                            Button::new("clear")
                                .ghost()
                                .small()
                                .icon(IconName::Delete)
                                .label("Clear")
                                .on_click(cx.listener(|this, _, window, cx| this.clear(window, cx))),
                        ),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w(px(120.))
                            .max_w(px(260.))
                            .child(
                                Input::new(&self.search)
                                    .w_full()
                                    .small()
                                    .cleanable(true)
                                    .prefix(Icon::new(IconName::Search).size_4()),
                            ),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .text_xs()
                            .truncate()
                            .text_color(cx.theme().muted_foreground)
                            .child(status),
                    ),
            )
            .child(
                div().flex_1().min_h_0().w_full().child(
                    v_resizable("messages-split")
                        .with_state(&self.split)
                        .child(
                            resizable_panel().child(
                                div()
                                    .size_full()
                                    .child(DataTable::new(&self.table).stripe(true).with_size(Size::Small)),
                            ),
                        )
                        .child(
                            resizable_panel()
                                .size(px(300.))
                                .size_range(px(120.)..px(1000.))
                                .child(
                                    div()
                                        .size_full()
                                        .border_t_1()
                                        .border_color(cx.theme().border)
                                        .child(self.detail.clone()),
                                ),
                        ),
                ),
            )
    }
}
