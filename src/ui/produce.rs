use std::rc::Rc;
use std::sync::Arc;

use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputState, Textarea, TextareaState};
use gpui_component::notification::Notification;
use gpui_component::select::{Select, SelectState};
use gpui_component::{ActiveTheme, IconName, IndexPath, Sizable, WindowExt, h_flex, v_flex};

use crate::kafka::metadata::TopicInfo;
use crate::kafka::produce::ProduceRequest;
use crate::kafka::worker::{Cmd, WorkerHandle};

pub struct ProduceView {
    worker: Option<Rc<WorkerHandle>>,
    topic: Option<Arc<TopicInfo>>,
    key: Entity<InputState>,
    value: Entity<TextareaState>,
    headers: Vec<(Entity<InputState>, Entity<InputState>)>,
    partition: Entity<SelectState<Vec<SharedString>>>,
    sending: bool,
    last_result: Option<String>,
}

impl ProduceView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let key = cx.new(|cx| InputState::new(window, cx).placeholder("Optional key"));
        let value = cx.new(|cx| TextareaState::new(window, cx).placeholder("Message value"));
        let partition = cx.new(|cx| {
            SelectState::new(vec![SharedString::from("Auto")], Some(IndexPath::default()), window, cx)
        });
        Self {
            worker: None,
            topic: None,
            key,
            value,
            headers: Vec::new(),
            partition,
            sending: false,
            last_result: None,
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
            let mut items = vec![SharedString::from("Auto")];
            if let Some(topic) = &self.topic {
                items.extend(topic.partitions.iter().map(|p| SharedString::from(format!("Partition {}", p.id))));
            }
            self.partition.update(cx, |select, cx| {
                select.set_items(items, window, cx);
                select.set_selected_index(Some(IndexPath::default()), window, cx);
            });
            self.last_result = None;
        }
        cx.notify();
    }

    fn add_header(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let key = cx.new(|cx| InputState::new(window, cx).placeholder("header name"));
        let value = cx.new(|cx| InputState::new(window, cx).placeholder("header value"));
        self.headers.push((key, value));
        cx.notify();
    }

    fn remove_header(&mut self, ix: usize, cx: &mut Context<Self>) {
        if ix < self.headers.len() {
            self.headers.remove(ix);
            cx.notify();
        }
    }

    fn build_request(&self, cx: &App) -> Option<ProduceRequest> {
        let topic = self.topic.as_ref()?;
        let partition_ix = self.partition.read(cx).selected_index(cx).map_or(0, |ix| ix.row);
        let partition = if partition_ix == 0 {
            None
        } else {
            topic.partitions.get(partition_ix - 1).map(|p| p.id)
        };
        let key = self.key.read(cx).value().to_string();
        let value = self.value.read(cx).value().to_string();
        let headers = self
            .headers
            .iter()
            .filter_map(|(k, v)| {
                let name = k.read(cx).value().trim().to_string();
                if name.is_empty() {
                    return None;
                }
                Some((name, Some(v.read(cx).value().as_bytes().to_vec())))
            })
            .collect();
        Some(ProduceRequest {
            topic: topic.name.clone(),
            partition,
            key: (!key.is_empty()).then(|| key.into_bytes()),
            value: Some(value.into_bytes()),
            headers,
        })
    }

    pub fn send_test_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.key.update(cx, |input, cx| input.set_value("kafkamitter", window, cx));
        self.value.update(cx, |input, cx| {
            input.set_value("{\"source\":\"kafkamitter\",\"hello\":true}", window, cx)
        });
        self.send(window, cx);
    }

    fn send(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (Some(worker), Some(request)) = (self.worker.clone(), self.build_request(cx)) else {
            return;
        };
        self.sending = true;
        self.last_result = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = worker.call(|reply| Cmd::Produce { request, reply }).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.sending = false;
                match result {
                    Ok(delivered) => {
                        let message = format!(
                            "Produced to partition {} at offset {}",
                            delivered.partition, delivered.offset
                        );
                        crate::startup::trace(&message);
                        view.last_result = Some(message.clone());
                        window.push_notification(Notification::success(message), cx);
                    }
                    Err(err) => {
                        let message = format!("Produce failed: {err}");
                        crate::startup::trace(&message);
                        view.last_result = Some(message.clone());
                        window.push_notification(Notification::error(message), cx);
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }
}

impl Render for ProduceView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let label = |text: &'static str, cx: &Context<Self>| {
            div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child(text)
        };
        let header_rows: Vec<AnyElement> = self
            .headers
            .iter()
            .enumerate()
            .map(|(ix, (key, value))| {
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(Input::new(key).w(px(220.)).small())
                    .child(Input::new(value).flex_1().small())
                    .child(
                        Button::new(("remove-header", ix))
                            .ghost()
                            .xsmall()
                            .icon(IconName::Close)
                            .on_click(cx.listener(move |this, _, _, cx| this.remove_header(ix, cx))),
                    )
                    .into_any_element()
            })
            .collect();
        v_flex()
            .size_full()
            .p_4()
            .gap_4()
            .max_w(px(960.))
            .child(
                v_flex()
                    .gap_1()
                    .child(label("Key", cx))
                    .child(Input::new(&self.key).w_full()),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(label("Value", cx))
                    .child(Textarea::new(&self.value).h(px(220.))),
            )
            .child(
                v_flex()
                    .gap_2()
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(label("Headers", cx))
                            .child(
                                Button::new("add-header")
                                    .ghost()
                                    .xsmall()
                                    .icon(IconName::Plus)
                                    .label("Add header")
                                    .on_click(cx.listener(|this, _, window, cx| this.add_header(window, cx))),
                            ),
                    )
                    .children(header_rows),
            )
            .child(
                h_flex()
                    .gap_3()
                    .items_center()
                    .child(label("Partition", cx))
                    .child(Select::new(&self.partition).w(px(160.)).small())
                    .child(div().flex_1())
                    .child(
                        Button::new("send")
                            .primary()
                            .icon(IconName::ArrowRight)
                            .label("Send")
                            .loading(self.sending)
                            .on_click(cx.listener(|this, _, window, cx| this.send(window, cx))),
                    ),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(self.last_result.clone().unwrap_or_default()),
            )
    }
}
