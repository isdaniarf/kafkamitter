use std::rc::Rc;
use std::sync::Arc;

use gpui::prelude::FluentBuilder as _;
use gpui::*;
use gpui_component::button::Button;
use gpui_component::switch::Switch;
use gpui_component::table::{Column, DataTable, TableDelegate, TableEvent, TableState};
use gpui_component::{ActiveTheme, IconName, Size, Sizable, h_flex, v_flex};

use crate::kafka::groups::{CommittedOffset, GroupSummary};
use crate::kafka::metadata::TopicInfo;
use crate::kafka::worker::{Cmd, WorkerHandle};
use crate::model::lag::{Lag, LagFlag, compute_lag};

#[derive(Clone, Debug)]
pub struct GroupRow {
    pub name: String,
    pub state: String,
    pub members: usize,
    pub partitions: usize,
    pub active: bool,
}

pub struct GroupsTableDelegate {
    rows: Vec<GroupRow>,
    columns: Vec<Column>,
}

impl GroupsTableDelegate {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            columns: vec![
                Column::new("name", "Group").width(px(260.)),
                Column::new("state", "State").width(px(110.)),
                Column::new("members", "Members").width(px(90.)).text_right(),
                Column::new("partitions", "Partitions").width(px(90.)).text_right(),
            ],
        }
    }

    fn text_for(&self, row: &GroupRow, col_ix: usize) -> String {
        match self.columns.get(col_ix).map(|c| c.key.as_ref()) {
            Some("name") => row.name.clone(),
            Some("state") => {
                if row.active {
                    row.state.clone()
                } else {
                    format!("{} (inactive)", row.state)
                }
            }
            Some("members") => row.members.to_string(),
            Some("partitions") => row.partitions.to_string(),
            _ => String::new(),
        }
    }
}

impl TableDelegate for GroupsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.rows.len()
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
        let Some(row) = self.rows.get(row_ix) else {
            return div().into_any_element();
        };
        let text = self.text_for(row, col_ix);
        div()
            .px_2()
            .text_sm()
            .truncate()
            .when(col_ix == 1 && !row.active, |el| el.text_color(cx.theme().muted_foreground))
            .child(text)
            .into_any_element()
    }

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        self.rows.get(row_ix).map(|r| self.text_for(r, col_ix)).unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct OffsetRow {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
    pub committed: Option<i64>,
    pub lag: Lag,
    pub member: String,
}

pub struct OffsetsTableDelegate {
    rows: Vec<OffsetRow>,
    columns: Vec<Column>,
}

impl OffsetsTableDelegate {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            columns: vec![
                Column::new("partition", "Part.").width(px(64.)).text_right(),
                Column::new("low", "Low").width(px(110.)).text_right(),
                Column::new("high", "High").width(px(110.)).text_right(),
                Column::new("committed", "Committed").width(px(110.)).text_right(),
                Column::new("lag", "Lag").width(px(120.)).text_right(),
                Column::new("member", "Member").width(px(420.)),
            ],
        }
    }

    fn text_for(&self, row: &OffsetRow, col_ix: usize) -> String {
        match self.columns.get(col_ix).map(|c| c.key.as_ref()) {
            Some("partition") => row.partition.to_string(),
            Some("low") => row.low.to_string(),
            Some("high") => row.high.to_string(),
            Some("committed") => row.committed.map_or("-".to_string(), |c| c.to_string()),
            Some("lag") => match (row.lag.value, row.lag.flag) {
                (None, _) => "-".to_string(),
                (Some(v), LagFlag::Expired) => format!("{v} (expired)"),
                (Some(v), LagFlag::Reset) => format!("{v} (reset)"),
                (Some(v), _) => v.to_string(),
            },
            Some("member") => row.member.clone(),
            _ => String::new(),
        }
    }
}

impl TableDelegate for OffsetsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.rows.len()
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
        let Some(row) = self.rows.get(row_ix) else {
            return div().into_any_element();
        };
        let text = self.text_for(row, col_ix);
        let lag_color = match (col_ix, row.lag.value, row.lag.flag) {
            (4, Some(0), LagFlag::None) => Some(cx.theme().green),
            (4, Some(_), LagFlag::None) => Some(cx.theme().yellow),
            (4, _, LagFlag::Expired | LagFlag::Reset) => Some(cx.theme().red),
            _ => None,
        };
        div()
            .px_2()
            .text_sm()
            .truncate()
            .when_some(lag_color, |el, color| el.text_color(color))
            .when(col_ix == 5, |el| el.text_color(cx.theme().muted_foreground))
            .child(text)
            .into_any_element()
    }

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        self.rows.get(row_ix).map(|r| self.text_for(r, col_ix)).unwrap_or_default()
    }
}

pub struct ConsumersView {
    worker: Option<Rc<WorkerHandle>>,
    topic: Option<Arc<TopicInfo>>,
    groups: Vec<GroupSummary>,
    groups_table: Entity<TableState<GroupsTableDelegate>>,
    offsets_table: Entity<TableState<OffsetsTableDelegate>>,
    selected_group: Option<String>,
    include_inactive: bool,
    loading: bool,
    scan_progress: Option<(usize, usize)>,
    total_lag: Option<i64>,
    generation: u64,
    last_error: Option<String>,
    needs_refresh: bool,
    pending_offsets_load: bool,
    _subscriptions: Vec<Subscription>,
}

impl ConsumersView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let groups_table = cx.new(|cx| {
            TableState::new(GroupsTableDelegate::new(), window, cx)
                .row_selectable(true)
                .col_resizable(true)
        });
        let offsets_table = cx.new(|cx| TableState::new(OffsetsTableDelegate::new(), window, cx).col_resizable(true));
        let subscriptions = vec![cx.subscribe_in(&groups_table, window, Self::on_groups_table_event)];
        Self {
            worker: None,
            topic: None,
            groups: Vec::new(),
            groups_table,
            offsets_table,
            selected_group: None,
            include_inactive: false,
            loading: false,
            scan_progress: None,
            total_lag: None,
            generation: 0,
            last_error: None,
            needs_refresh: false,
            pending_offsets_load: false,
            _subscriptions: subscriptions,
        }
    }

    pub fn set_include_inactive(&mut self, include: bool) {
        self.include_inactive = include;
    }

    pub fn ensure_loaded(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.needs_refresh && self.topic.is_some() {
            self.needs_refresh = false;
            self.refresh(window, cx);
        }
        if self.pending_offsets_load {
            self.pending_offsets_load = false;
            self.load_offsets(window, cx);
        }
    }

    pub fn set_topic(
        &mut self,
        worker: Option<Rc<WorkerHandle>>,
        topic: Option<Arc<TopicInfo>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let changed = self.topic.as_ref().map(|t| t.name.as_str()) != topic.as_ref().map(|t| t.name.as_str());
        self.worker = worker;
        self.topic = topic;
        if changed {
            self.generation += 1;
            self.groups.clear();
            self.selected_group = None;
            self.total_lag = None;
            self.last_error = None;
            self.scan_progress = None;
            self.set_group_rows(Vec::new(), cx);
            self.set_offset_rows(Vec::new(), cx);
            self.needs_refresh = self.topic.is_some();
        }
        let _ = window;
        cx.notify();
    }

    fn set_group_rows(&mut self, rows: Vec<GroupRow>, cx: &mut Context<Self>) {
        self.groups_table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            table.refresh(cx);
        });
    }

    fn set_offset_rows(&mut self, rows: Vec<OffsetRow>, cx: &mut Context<Self>) {
        self.offsets_table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            table.refresh(cx);
        });
    }

    pub fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (Some(worker), Some(topic)) = (self.worker.clone(), self.topic.clone()) else {
            return;
        };
        self.generation += 1;
        let generation = self.generation;
        self.loading = true;
        self.last_error = None;
        self.scan_progress = None;
        let include_inactive = self.include_inactive;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let groups = worker.call(|reply| Cmd::ListGroups { reply }).await;
            let groups = match groups {
                Ok(groups) => groups,
                Err(err) => {
                    let _ = this.update_in(cx, |view, _, cx| {
                        if view.generation == generation {
                            view.loading = false;
                            view.last_error = Some(err.to_string());
                            cx.notify();
                        }
                    });
                    return;
                }
            };
            let (active, others): (Vec<GroupSummary>, Vec<GroupSummary>) =
                groups.into_iter().partition(|g| g.consumes(&topic.name));
            let mut rows: Vec<GroupRow> = active
                .iter()
                .map(|g| GroupRow {
                    name: g.name.clone(),
                    state: g.state.clone(),
                    members: g.members.len(),
                    partitions: g.partitions_of(&topic.name).len(),
                    active: true,
                })
                .collect();
            crate::startup::trace(&format!(
                "groups: {} active on {}, {} others",
                rows.len(),
                topic.name,
                others.len()
            ));
            let mut all = active;
            all.extend(others.iter().cloned());
            let stored = this
                .update_in(cx, |view, window, cx| {
                    if view.generation != generation {
                        return false;
                    }
                    view.groups = all.clone();
                    view.loading = false;
                    if include_inactive {
                        view.scan_progress = Some((0, others.len()));
                    }
                    view.set_group_rows(rows.clone(), cx);
                    view.reselect(cx);
                    if view.selected_group.is_none() && std::env::var_os("KAFKAMITTER_DEV_SELECT_GROUP").is_some() {
                        if let Some(first) = rows.first() {
                            view.selected_group = Some(first.name.clone());
                            view.groups_table.update(cx, |table, cx| table.set_selected_row(0, cx));
                            view.pending_offsets_load = true;
                            view.ensure_loaded(window, cx);
                        }
                    }
                    cx.notify();
                    true
                })
                .unwrap_or(false);
            if !stored {
                return;
            }
            if !include_inactive {
                return;
            }
            let partitions = topic.partition_ids();
            let names: Vec<String> = others.iter().map(|g| g.name.clone()).collect();
            let total = names.len();
            let (results_tx, results_rx) = smol::channel::bounded::<(String, anyhow::Result<Vec<CommittedOffset>>)>(64);
            let scan_worker = worker.clone();
            let scan_topic = topic.name.clone();
            let scan = cx.spawn(async move |_| {
                scan_worker
                    .call(|reply| Cmd::ScanGroupOffsets {
                        groups: names,
                        topic: scan_topic,
                        partitions,
                        results: results_tx,
                        reply,
                    })
                    .await
            });
            let started = std::time::Instant::now();
            let mut done = 0usize;
            while let Ok((name, result)) = results_rx.recv().await {
                done += 1;
                let committed_any = match result {
                    Ok(offsets) => offsets.iter().filter(|o| o.committed.is_some()).count(),
                    Err(_) => 0,
                };
                if committed_any > 0 {
                    if let Some(group) = others.iter().find(|g| g.name == name) {
                        rows.push(GroupRow {
                            name: group.name.clone(),
                            state: group.state.clone(),
                            members: group.members.len(),
                            partitions: committed_any,
                            active: false,
                        });
                    }
                }
                let keep_going = this
                    .update_in(cx, |view, _, cx| {
                        if view.generation != generation {
                            return false;
                        }
                        view.scan_progress = Some((done, total));
                        if committed_any > 0 {
                            view.set_group_rows(rows.clone(), cx);
                        }
                        cx.notify();
                        true
                    })
                    .unwrap_or(false);
                if !keep_going {
                    return;
                }
            }
            let outcome = scan.await;
            crate::startup::trace(&format!(
                "group scan: {} of {} groups checked, {} inactive with commits, {:.0} ms",
                done,
                total,
                rows.iter().filter(|r| !r.active).count(),
                started.elapsed().as_secs_f64() * 1000.0
            ));
            let _ = this.update_in(cx, |view, window, cx| {
                if view.generation == generation {
                    view.scan_progress = None;
                    if let Err(err) = outcome {
                        view.last_error = Some(err.to_string());
                    }
                    if view.selected_group.is_none() && std::env::var_os("KAFKAMITTER_DEV_SELECT_GROUP").is_some() {
                        if let Some(first) = rows.first() {
                            view.selected_group = Some(first.name.clone());
                            view.groups_table.update(cx, |table, cx| table.set_selected_row(0, cx));
                            view.load_offsets(window, cx);
                        }
                    }
                    cx.notify();
                }
            });
        })
        .detach();
    }

    fn reselect(&mut self, cx: &mut Context<Self>) {
        let Some(selected) = self.selected_group.clone() else {
            return;
        };
        let row = self
            .groups_table
            .read(cx)
            .delegate()
            .rows
            .iter()
            .position(|r| r.name == selected);
        match row {
            Some(row_ix) => self.groups_table.update(cx, |table, cx| table.set_selected_row(row_ix, cx)),
            None => {
                self.selected_group = None;
                self.total_lag = None;
                self.set_offset_rows(Vec::new(), cx);
            }
        }
    }

    fn on_groups_table_event(
        &mut self,
        table: &Entity<TableState<GroupsTableDelegate>>,
        event: &TableEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let TableEvent::SelectRow(row_ix) | TableEvent::DoubleClickedRow(row_ix) = event {
            let name = table.read(cx).delegate().rows.get(*row_ix).map(|r| r.name.clone());
            if let Some(name) = name {
                if self.selected_group.as_deref() != Some(name.as_str()) {
                    self.selected_group = Some(name);
                    self.load_offsets(window, cx);
                }
            }
        }
    }

    fn load_offsets(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (Some(worker), Some(topic), Some(group)) =
            (self.worker.clone(), self.topic.clone(), self.selected_group.clone())
        else {
            return;
        };
        let generation = self.generation;
        let members: Vec<(i32, String)> = self
            .groups
            .iter()
            .find(|g| g.name == group)
            .map(|g| {
                g.partitions_of(&topic.name)
                    .into_iter()
                    .map(|(p, m)| (p, format!("{} ({})", m.client_id, m.client_host)))
                    .collect()
            })
            .unwrap_or_default();
        let partitions = topic.partition_ids();
        self.total_lag = None;
        self.set_offset_rows(Vec::new(), cx);
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let watermarks = worker
                .call(|reply| Cmd::FetchWatermarks {
                    topic: topic.name.clone(),
                    partitions: partitions.clone(),
                    reply,
                })
                .await;
            let committed = worker
                .call(|reply| Cmd::CommittedOffsets {
                    group: group.clone(),
                    topic: topic.name.clone(),
                    partitions: partitions.clone(),
                    reply,
                })
                .await;
            let result = watermarks.and_then(|watermarks| committed.map(|committed| (watermarks, committed)));
            let _ = this.update_in(cx, |view, _, cx| {
                if view.generation != generation || view.selected_group.as_deref() != Some(group.as_str()) {
                    return;
                }
                match result {
                    Ok((watermarks, committed)) => {
                        let rows: Vec<OffsetRow> = watermarks
                            .iter()
                            .map(|w| {
                                let committed = committed
                                    .iter()
                                    .find(|c| c.partition == w.partition)
                                    .and_then(|c| c.committed);
                                OffsetRow {
                                    partition: w.partition,
                                    low: w.low,
                                    high: w.high,
                                    committed,
                                    lag: compute_lag(w.low, w.high, committed),
                                    member: members
                                        .iter()
                                        .find(|(p, _)| *p == w.partition)
                                        .map(|(_, m)| m.clone())
                                        .unwrap_or_default(),
                                }
                            })
                            .collect();
                        let total: i64 = rows.iter().filter_map(|r| r.lag.value).sum();
                        crate::startup::trace(&format!(
                            "offsets for {group}: {} partitions, total lag {total}",
                            rows.len()
                        ));
                        view.total_lag = Some(total);
                        view.set_offset_rows(rows, cx);
                    }
                    Err(err) => {
                        view.last_error = Some(err.to_string());
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn status_text(&self) -> String {
        let mut parts = Vec::new();
        if self.loading {
            parts.push("Loading groups".to_string());
        }
        if let Some((done, total)) = self.scan_progress {
            parts.push(format!("Scanning inactive groups {done}/{total}"));
        }
        if let Some(err) = &self.last_error {
            parts.push(format!("error: {err}"));
        }
        parts.join("  ·  ")
    }
}

impl Render for ConsumersView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let status = self.status_text();
        let offsets_title = match (&self.selected_group, self.total_lag) {
            (Some(group), Some(total)) => format!("{group}  ·  total lag {total}"),
            (Some(group), None) => group.clone(),
            (None, _) => "Select a group to see its offsets".to_string(),
        };
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .w_full()
                    .px_3()
                    .py_2()
                    .gap_3()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        Button::new("refresh-groups")
                            .small()
                            .icon(IconName::RotateCw)
                            .label("Refresh")
                            .loading(self.loading)
                            .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
                    )
                    .child(
                        Switch::new("include-inactive")
                            .checked(self.include_inactive)
                            .label("Include inactive groups")
                            .on_change(cx.listener(|this, checked: &bool, window, cx| {
                                this.include_inactive = *checked;
                                this.refresh(window, cx);
                            })),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(status),
                    ),
            )
            .child(
                h_flex()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .child(
                        div()
                            .w(px(560.))
                            .h_full()
                            .border_r_1()
                            .border_color(cx.theme().border)
                            .child(DataTable::new(&self.groups_table).stripe(true).with_size(Size::Small)),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .h_full()
                            .child(
                                div()
                                    .px_3()
                                    .py_2()
                                    .text_sm()
                                    .border_b_1()
                                    .border_color(cx.theme().border)
                                    .child(offsets_title),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .min_h_0()
                                    .child(DataTable::new(&self.offsets_table).stripe(true).with_size(Size::Small)),
                            ),
                    ),
            )
    }
}
