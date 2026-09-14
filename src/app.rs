use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use gpui::prelude::FluentBuilder as _;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::list::{List, ListEvent, ListState};
use gpui_component::menu::{ContextMenuExt, PopupMenuItem};
use gpui_component::notification::Notification;
use gpui_component::resizable::{ResizableState, h_resizable, resizable_panel};
use gpui_component::tab::{Tab, TabBar};
use gpui_component::{ActiveTheme, IconName, ThemeMode, Root, Size, Sizable, StyledExt, Theme, TitleBar, WindowExt, h_flex, v_flex};

use crate::kafka::KafkaService;
use crate::kafka::metadata::{ClusterInfo, TopicInfo};
use crate::kafka::worker::{Cmd, WorkerHandle};
use crate::model::keychain;
use crate::model::profile::{ConnectionProfile, Security, load_profiles, profiles_path, save_profiles};
use crate::ui::connections::open_connection_dialog;
use crate::ui::messages::MessagesView;
use crate::ui::produce::ProduceView;
use crate::ui::consumers::ConsumersView;
use crate::ui::topics::{CreateTopic, DeleteTopic, open_create_topic_dialog};
use crate::ui::settings::open_settings_dialog;
use crate::model::settings::{Settings, ThemeChoice, load_settings, save_settings, settings_path};
use gpui_component::menu::DropdownMenu as _;
use crate::kafka::admin::NewTopicSpec;
use crate::kafka::import::prepare_import;
use crate::model::profile::ca_dir;
use crate::ui::topics::TopicListDelegate;

const DEV_PROFILE_ID: &str = "dev";

actions!(
    kafkamitter,
    [
        Quit,
        OpenSettings,
        GoToTop,
        GoToBottom,
        FocusMessageValue,
        FocusSearch,
        NextConnection,
        PreviousConnection,
        EditActiveConnection,
        ConsumeSelected,
        StopConsume,
        NewTab,
        DuplicateTab,
        CloseTab,
        CloseAllTabs
    ]
);

#[derive(Clone, PartialEq, Debug, Action)]
#[action(namespace = kafkamitter, no_json)]
pub struct SwitchConnection {
    pub index: usize,
}


#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum MainTab {
    #[default]
    Messages,
    Produce,
    Consumers,
}

impl MainTab {
    const ALL: [MainTab; 3] = [MainTab::Messages, MainTab::Produce, MainTab::Consumers];

    fn label(self) -> &'static str {
        match self {
            MainTab::Messages => "Messages",
            MainTab::Produce => "Produce",
            MainTab::Consumers => "Consumers",
        }
    }
}

/// What the user was looking at on a connection, so a switch can return to it.
#[derive(Clone, Default)]
struct ConnectionView {
    topic: Option<String>,
    tab: MainTab,
}

/// The largest number of tabs the window keeps.
pub const MAX_TABS: usize = 13;

/// One tab. It owns its own views, so a switch between tabs reloads nothing.
struct Session {
    id: u64,
    connection: Option<String>,
    topic: Option<Arc<TopicInfo>>,
    tab: MainTab,
    messages: Entity<MessagesView>,
    produce: Entity<ProduceView>,
    consumers: Entity<ConsumersView>,
}

impl Session {
    fn new(id: u64, window: &mut Window, cx: &mut Context<KafkamitterApp>) -> Self {
        Self {
            id,
            connection: None,
            topic: None,
            tab: MainTab::Messages,
            messages: cx.new(|cx| MessagesView::new(window, cx)),
            produce: cx.new(|cx| ProduceView::new(window, cx)),
            consumers: cx.new(|cx| ConsumersView::new(window, cx)),
        }
    }
}

pub enum ConnState {
    Connecting,
    Connected(Rc<ClusterInfo>),
    Failed(String),
}

pub struct KafkamitterApp {
    kafka: KafkaService,
    profiles: Vec<ConnectionProfile>,
    states: HashMap<String, ConnState>,
    views: HashMap<String, ConnectionView>,
    dev_passwords: HashMap<String, String>,
    sessions: Vec<Session>,
    current: usize,
    next_session_id: u64,
    topics: Entity<ListState<TopicListDelegate>>,
    split: Entity<ResizableState>,
    settings: Settings,
    focus_handle: FocusHandle,
    reselect_after_refresh: Option<String>,
    dev_switch_back: Option<String>,
    dev_retried: bool,
    theme_preview: Option<Subscription>,
    first_render_traced: bool,
    _subscriptions: Vec<Subscription>,
}

impl KafkamitterApp {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let topics = cx.new(|cx| ListState::new(TopicListDelegate::new(), window, cx).searchable(true));
        let first_session = Session::new(0, window, cx);
        let split = cx.new(|_| ResizableState::default());
        let focus_handle = cx.focus_handle();
        window.focus(&focus_handle, cx);
        let subscriptions = vec![
            cx.subscribe_in(&topics, window, Self::on_topic_list_event),
            cx.observe_window_appearance(window, |this, window, cx| {
                if this.settings.theme == ThemeChoice::System {
                    Theme::sync_system_appearance(Some(window), cx);
                }
            }),
        ];
        Theme::sync_system_appearance(Some(window), cx);
        cx.spawn_in(window, async move |this, cx| {
            let loaded = load_profiles(&profiles_path());
            let _ = this.update_in(cx, |app, window, cx| app.finish_loading(loaded, window, cx));
        })
        .detach();
        Self {
            kafka: KafkaService::new(),
            profiles: Vec::new(),
            states: HashMap::new(),
            views: HashMap::new(),
            dev_passwords: HashMap::new(),
            sessions: vec![first_session],
            current: 0,
            next_session_id: 1,
            topics,
            split,
            settings: Settings::default(),
            focus_handle,
            reselect_after_refresh: None,
            dev_switch_back: None,
            dev_retried: false,
            theme_preview: None,
            first_render_traced: false,
            _subscriptions: subscriptions,
        }
    }

    fn session(&self) -> &Session {
        &self.sessions[self.current.min(self.sessions.len() - 1)]
    }

    fn session_mut(&mut self) -> &mut Session {
        let ix = self.current.min(self.sessions.len() - 1);
        &mut self.sessions[ix]
    }

    fn messages(&self) -> Entity<MessagesView> {
        self.session().messages.clone()
    }

    fn produce(&self) -> Entity<ProduceView> {
        self.session().produce.clone()
    }

    fn consumers(&self) -> Entity<ConsumersView> {
        self.session().consumers.clone()
    }

    fn session_label(&self, session: &Session) -> String {
        if let Some(topic) = &session.topic {
            return topic.name.clone();
        }
        if let Some(id) = &session.connection {
            if let Some(profile) = self.profiles.iter().find(|p| p.id == *id) {
                return profile.name.clone();
            }
        }
        "New tab".to_string()
    }

    /// Points the sidebar at the current tab. It reads the cached cluster only,
    /// so a switch between tabs asks the broker for nothing.
    fn sync_sidebar_to_session(&mut self, cx: &mut Context<Self>) {
        let topics: Vec<Arc<TopicInfo>> = match self
            .session()
            .connection
            .as_deref()
            .and_then(|id| self.states.get(id))
        {
            Some(ConnState::Connected(cluster)) => cluster.topics.iter().cloned().map(Arc::new).collect(),
            _ => Vec::new(),
        };
        self.set_topics(topics, cx);
        let selected = self.session().topic.as_ref().map(|t| t.name.clone());
        if let Some(name) = selected {
            self.topics.update(cx, |list, cx| {
                list.delegate_mut().select_topic(&name);
                cx.notify();
            });
        }
    }

    fn add_session(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.sessions.len() >= MAX_TABS {
            window.push_notification(
                Notification::warning(format!("A window holds at most {MAX_TABS} tabs")),
                cx,
            );
            return false;
        }
        let id = self.next_session_id;
        self.next_session_id += 1;
        self.sessions.push(Session::new(id, window, cx));
        self.current = self.sessions.len() - 1;
        true
    }

    fn on_new_tab(&mut self, _: &NewTab, window: &mut Window, cx: &mut Context<Self>) {
        if self.add_session(window, cx) {
            self.sync_sidebar_to_session(cx);
            crate::startup::trace(&format!("new tab: {} of {MAX_TABS}", self.sessions.len()));
            cx.notify();
        }
    }

    fn on_duplicate_tab(&mut self, _: &DuplicateTab, window: &mut Window, cx: &mut Context<Self>) {
        self.duplicate_session(self.current, window, cx);
    }

    fn duplicate_session(&mut self, ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(source) = self.sessions.get(ix) else {
            return;
        };
        let (connection, topic, tab) = (source.connection.clone(), source.topic.clone(), source.tab);
        if !self.add_session(window, cx) {
            return;
        }
        {
            let session = self.session_mut();
            session.connection = connection;
            session.topic = topic;
            session.tab = tab;
        }
        self.sync_sidebar_to_session(cx);
        self.sync_messages_view(window, cx);
        crate::startup::trace(&format!("duplicated tab: {} of {MAX_TABS}", self.sessions.len()));
        cx.notify();
    }

    fn close_session(&mut self, ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        if ix >= self.sessions.len() {
            return;
        }
        self.sessions.remove(ix);
        if self.sessions.is_empty() {
            let id = self.next_session_id;
            self.next_session_id += 1;
            self.sessions.push(Session::new(id, window, cx));
            self.current = 0;
        } else if self.current > ix || self.current >= self.sessions.len() {
            self.current = self.current.saturating_sub(1).min(self.sessions.len() - 1);
        }
        self.sync_sidebar_to_session(cx);
        cx.notify();
    }

    fn on_close_tab(&mut self, _: &CloseTab, window: &mut Window, cx: &mut Context<Self>) {
        let ix = self.current;
        self.close_session(ix, window, cx);
    }

    fn on_close_all_tabs(&mut self, _: &CloseAllTabs, window: &mut Window, cx: &mut Context<Self>) {
        self.sessions.clear();
        let id = self.next_session_id;
        self.next_session_id += 1;
        self.sessions.push(Session::new(id, window, cx));
        self.current = 0;
        self.sync_sidebar_to_session(cx);
        cx.notify();
    }

    /// Shows another tab. It touches no view state, so nothing reloads.
    fn select_session(&mut self, ix: usize, cx: &mut Context<Self>) {
        if ix >= self.sessions.len() || ix == self.current {
            return;
        }
        self.current = ix;
        crate::startup::trace(&format!(
            "select tab {ix}: connection={} topic={}",
            self.session().connection.as_deref().unwrap_or("-"),
            self.session().topic.as_ref().map_or("-", |t| t.name.as_str())
        ));
        self.sync_sidebar_to_session(cx);
        cx.notify();
    }

    fn finish_loading(
        &mut self,
        loaded: anyhow::Result<Vec<ConnectionProfile>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match loaded {
            Ok(profiles) => self.profiles = profiles,
            Err(err) => window.push_notification(
                Notification::error(format!("Cannot read saved connections: {err}")),
                cx,
            ),
        }
        self.migrate_legacy_passwords(window, cx);
        self.settings = load_settings(&settings_path());
        self.apply_theme(window, cx);
        let settings = self.settings.clone();
        self.messages().update(cx, |view, cx| view.apply_settings(&settings, window, cx));
        if let Ok(bootstrap) = std::env::var("KAFKAMITTER_DEV_BOOTSTRAP") {
            self.profiles.push(ConnectionProfile {
                id: DEV_PROFILE_ID.into(),
                name: "dev".into(),
                bootstrap_servers: bootstrap,
                security: Security::Plaintext,
                ca_location: None,
                password: None,
            });
            self.connect(DEV_PROFILE_ID.to_string(), window, cx);
        }
        if let Ok(path) = std::env::var("KAFKAMITTER_DEV_IMPORT") {
            match prepare_import(std::path::Path::new(&path), &ca_dir()) {
                Ok(imported) => {
                    let mut profile = imported.profile;
                    profile.id = format!("{DEV_PROFILE_ID}-{}", profile.name);
                    if let Some(password) = imported.password {
                        self.dev_passwords.insert(profile.id.clone(), password);
                    }
                    let id = profile.id.clone();
                    self.profiles.retain(|p| p.id != id);
                    self.profiles.push(profile);
                    self.connect(id, window, cx);
                }
                Err(err) => crate::startup::trace(&format!("dev import failed: {err}")),
            }
        }
        if std::env::var_os("KAFKAMITTER_DEV_SETTINGS").is_some() {
            self.on_open_settings(&OpenSettings, window, cx);
        }
        if let Ok(spec) = std::env::var("KAFKAMITTER_DEV_RENAME") {
            if let Some((old, new)) = spec.split_once('=') {
                match self.profiles.iter().find(|p| p.name == old).cloned() {
                    Some(profile) => {
                        let form = crate::ui::connections::ConnectionForm::new(Some(&profile), None, window, cx);
                        form.set_name(new, window, cx);
                        match form.build(cx) {
                            Ok((built, password)) => {
                                let result = self.upsert_profile(built, password, window, cx);
                                crate::startup::trace(&format!("rename {old} -> {new}: {result:?}"));
                            }
                            Err(err) => crate::startup::trace(&format!("rename build failed: {err}")),
                        }
                    }
                    None => crate::startup::trace(&format!("no profile named {old}")),
                }
            }
        }
        if let Ok(name) = std::env::var("KAFKAMITTER_DEV_EDIT") {
            if let Some(id) = self.profiles.iter().find(|p| p.name == name).map(|p| p.id.clone()) {
                self.edit_profile(&id, window, cx);
            } else {
                crate::startup::trace(&format!("no profile named {name} to edit"));
            }
        }
        if let Ok(name) = std::env::var("KAFKAMITTER_DEV_CONNECT") {
            if let Some(id) = self.profiles.iter().find(|p| p.name == name).map(|p| p.id.clone()) {
                self.connect(id, window, cx);
            } else {
                crate::startup::trace(&format!("no saved connection named {name}"));
            }
        }
        cx.notify();
    }

    /// Moves passwords that an older version stored in the macOS Keychain into the
    /// connections file, then removes the Keychain items. This runs once.
    fn migrate_legacy_passwords(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let pending: Vec<String> = self
            .profiles
            .iter()
            .filter(|p| p.needs_password() && p.password.is_none())
            .map(|p| p.id.clone())
            .collect();
        if pending.is_empty() {
            return;
        }
        cx.spawn_in(window, async move |this, cx| {
            let found = cx
                .background_spawn(async move {
                    pending
                        .into_iter()
                        .filter_map(|id| {
                            let password = keychain::get_password(&id).ok().flatten()?;
                            let _ = keychain::delete_password(&id);
                            Some((id, password))
                        })
                        .collect::<Vec<_>>()
                })
                .await;
            let _ = this.update_in(cx, |app, window, cx| {
                if found.is_empty() {
                    return;
                }
                for (id, password) in &found {
                    if let Some(profile) = app.profiles.iter_mut().find(|p| p.id == *id) {
                        profile.password = Some(password.clone());
                    }
                }
                crate::startup::trace(&format!("migrated {} keychain passwords", found.len()));
                match app.save() {
                    Ok(()) => window.push_notification(
                        Notification::success(format!(
                            "Moved {} password(s) from the Keychain into the connections file",
                            found.len()
                        )),
                        cx,
                    ),
                    Err(err) => window.push_notification(
                        Notification::error(format!("Cannot save connections: {err}")),
                        cx,
                    ),
                }
                cx.notify();
            });
        })
        .detach();
    }

    /// Puts the window into the given appearance without saving anything.
    pub fn preview_theme(choice: ThemeChoice, window: &mut Window, cx: &mut App) {
        match choice {
            ThemeChoice::System => Theme::sync_system_appearance(Some(window), cx),
            ThemeChoice::Light => Theme::change(ThemeMode::Light, Some(window), cx),
            ThemeChoice::Dark => Theme::change(ThemeMode::Dark, Some(window), cx),
        }
    }

    /// Puts the window into the appearance the saved settings ask for.
    pub fn apply_theme(&self, window: &mut Window, cx: &mut App) {
        Self::preview_theme(self.settings.theme, window, cx);
    }

    pub fn apply_settings(&mut self, settings: Settings, window: &mut Window, cx: &mut Context<Self>) {
        if let Err(err) = save_settings(&settings_path(), &settings) {
            window.push_notification(Notification::error(format!("Cannot save settings: {err}")), cx);
        }
        self.settings = settings.clone();
        self.apply_theme(window, cx);
        let views: Vec<_> = self.sessions.iter().map(|s| s.messages.clone()).collect();
        for view in views {
            view.update(cx, |view, cx| view.apply_settings(&settings, window, cx));
        }
        cx.notify();
    }

    fn on_open_settings(&mut self, _: &OpenSettings, window: &mut Window, cx: &mut Context<Self>) {
        let subscription =
            open_settings_dialog(cx.entity().downgrade(), self.settings.clone(), window, cx);
        self.theme_preview = Some(subscription);
    }

    fn on_go_to_top(&mut self, _: &GoToTop, window: &mut Window, cx: &mut Context<Self>) {
        self.messages().update(cx, |view, cx| view.go_to_top(window, cx));
    }

    fn on_go_to_bottom(&mut self, _: &GoToBottom, window: &mut Window, cx: &mut Context<Self>) {
        self.messages().update(cx, |view, cx| view.go_to_bottom(window, cx));
    }

    fn on_focus_message_value(&mut self, _: &FocusMessageValue, window: &mut Window, cx: &mut Context<Self>) {
        if self.session().tab == MainTab::Messages {
            self.messages().update(cx, |view, cx| view.focus_value(window, cx));
        }
    }

    fn on_focus_search(&mut self, _: &FocusSearch, window: &mut Window, cx: &mut Context<Self>) {
        if self.session().topic.is_none() {
            return;
        }
        self.session_mut().tab = MainTab::Messages;
        self.messages().update(cx, |view, cx| view.focus_search(window, cx));
        cx.notify();
    }

    fn on_consume_selected(&mut self, _: &ConsumeSelected, window: &mut Window, cx: &mut Context<Self>) {
        if self.session().topic.is_some() {
            self.session_mut().tab = MainTab::Messages;
            self.messages().update(cx, |view, cx| view.start_default(window, cx));
            cx.notify();
        }
    }

    fn on_stop_consume(&mut self, _: &StopConsume, _window: &mut Window, cx: &mut Context<Self>) {
        self.messages().update(cx, |view, cx| view.stop(cx));
    }

    fn switch_connection_by_offset(&mut self, offset: isize, window: &mut Window, cx: &mut Context<Self>) {
        if self.profiles.is_empty() {
            return;
        }
        let len = self.profiles.len() as isize;
        let current = self
            .session()
            .connection
            .as_deref()
            .and_then(|id| self.profiles.iter().position(|p| p.id == id))
            .map_or(-1, |ix| ix as isize);
        let next = (current + offset).rem_euclid(len) as usize;
        let id = self.profiles[next].id.clone();
        self.activate_connection(id, window, cx);
    }

    fn on_next_connection(&mut self, _: &NextConnection, window: &mut Window, cx: &mut Context<Self>) {
        self.switch_connection_by_offset(1, window, cx);
    }

    fn on_previous_connection(&mut self, _: &PreviousConnection, window: &mut Window, cx: &mut Context<Self>) {
        self.switch_connection_by_offset(-1, window, cx);
    }

    fn on_switch_connection(&mut self, action: &SwitchConnection, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(profile) = self.profiles.get(action.index) {
            let id = profile.id.clone();
            self.activate_connection(id, window, cx);
        }
    }

    fn on_edit_active_connection(&mut self, _: &EditActiveConnection, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(id) = self.session().connection.clone() {
            self.edit_profile(&id, window, cx);
        }
    }

    fn save(&self) -> anyhow::Result<()> {
        let persisted: Vec<ConnectionProfile> = self
            .profiles
            .iter()
            .filter(|p| p.id != DEV_PROFILE_ID && !p.id.starts_with(&format!("{DEV_PROFILE_ID}-")))
            .cloned()
            .collect();
        save_profiles(&profiles_path(), &persisted)
    }

    pub fn upsert_profile(
        &mut self,
        mut profile: ConnectionProfile,
        password: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        profile.password = password.filter(|p| !p.is_empty());
        let id = profile.id.clone();
        match self.profiles.iter_mut().find(|p| p.id == id) {
            Some(slot) => *slot = profile,
            None => self.profiles.push(profile),
        }
        self.disconnect(&id, window, cx);
        let result = self.save();
        cx.notify();
        result
    }

    fn remove_profile(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.disconnect(id, window, cx);
        self.profiles.retain(|p| p.id != id);
        self.views.remove(id);
        let legacy_id = id.to_string();
        cx.background_spawn(async move {
            let _ = keychain::delete_password(&legacy_id);
        })
        .detach();
        if let Err(err) = self.save() {
            window.push_notification(Notification::error(format!("Cannot save connections: {err}")), cx);
        }
        cx.notify();
    }

    fn disconnect(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if self.session().connection.as_deref() == Some(id) {
            self.remember_current_view();
        }
        self.kafka.disconnect(id);
        self.states.remove(id);
        if self.session().connection.as_deref() == Some(id) {
            self.session_mut().connection = None;
            self.session_mut().topic = None;
            self.set_topics(Vec::new(), cx);
            self.sync_messages_view(window, cx);
        }
        cx.notify();
    }

    fn import_properties(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: Some("Import".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = receiver.await else {
                return;
            };
            for path in paths {
                let imported = prepare_import(&path, &ca_dir());
                let _ = this.update_in(cx, |app, window, cx| match imported {
                    Ok(imported) => {
                        let mut profile = imported.profile;
                        if let Some(existing) = app.profiles.iter().find(|p| p.name == profile.name) {
                            profile.id = existing.id.clone();
                        }
                        let name = profile.name.clone();
                        match app.upsert_profile(profile, imported.password, window, cx) {
                            Ok(()) => window.push_notification(Notification::success(format!("Imported {name}")), cx),
                            Err(err) => window.push_notification(Notification::error(format!("Import failed: {err}")), cx),
                        }
                    }
                    Err(err) => window.push_notification(Notification::error(format!("Import failed: {err}")), cx),
                });
            }
        })
        .detach();
    }

    fn open_new_connection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        open_connection_dialog(cx.entity().downgrade(), None, None, window, cx);
    }

    fn edit_profile(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(profile) = self.profiles.iter().find(|p| p.id == id).cloned() else {
            return;
        };
        let password = profile.password.clone();
        open_connection_dialog(cx.entity().downgrade(), Some(profile), password, window, cx);
    }

    /// Stores the topic and the tab of the active connection before leaving it.
    fn remember_current_view(&mut self) {
        if let Some(id) = self.session().connection.clone() {
            self.views.insert(
                id,
                ConnectionView {
                    topic: self.session().topic.as_ref().map(|t| t.name.clone()),
                    tab: self.session().tab,
                },
            );
        }
    }

    /// Selects the topic and the tab that this connection had before.
    fn restore_view(&mut self, id: &str, cx: &mut Context<Self>) {
        let Some(view) = self.views.get(id).cloned() else {
            return;
        };
        self.session_mut().tab = view.tab;
        if let Some(name) = view.topic {
            self.select_topic_by_name(&name, cx);
            if self.session().topic.is_some() {
                crate::startup::trace(&format!("restored view: {name} on tab {}", self.session().tab.label()));
            }
        }
    }

    /// Drops the worker and connects again. Used after a failure and by the
    /// Reconnect menu item.
    fn reconnect(&mut self, id: String, window: &mut Window, cx: &mut Context<Self>) {
        crate::startup::trace(&format!("reconnect requested for {id}"));
        self.kafka.disconnect(&id);
        self.states.remove(&id);
        self.connect(id, window, cx);
    }

    fn activate_connection(&mut self, id: String, window: &mut Window, cx: &mut Context<Self>) {
        if self.session().connection.as_deref() == Some(id.as_str()) {
            return;
        }
        self.remember_current_view();
        match self.states.get(&id) {
            Some(ConnState::Connected(cluster)) => {
                let topics = cluster.topics.iter().cloned().map(Arc::new).collect();
                self.session_mut().connection = Some(id.clone());
                self.session_mut().topic = None;
                self.set_topics(topics, cx);
                self.restore_view(&id, cx);
                self.sync_messages_view(window, cx);
                cx.notify();
            }
            Some(ConnState::Connecting) => {
                self.session_mut().connection = Some(id);
                cx.notify();
            }
            _ => self.connect(id, window, cx),
        }
    }

    fn connect(&mut self, id: String, window: &mut Window, cx: &mut Context<Self>) {
        let Some(profile) = self.profiles.iter().find(|p| p.id == id).cloned() else {
            return;
        };
        let password = self
            .dev_passwords
            .get(&profile.id)
            .cloned()
            .or_else(|| profile.password.clone());
        if self.session().connection.as_deref() != Some(id.as_str()) {
            self.remember_current_view();
        }
        let worker = self.kafka.worker(&profile, password.as_deref());
        self.states.insert(id.clone(), ConnState::Connecting);
        self.session_mut().connection = Some(id.clone());
        self.session_mut().topic = None;
        self.set_topics(Vec::new(), cx);
        self.sync_messages_view(window, cx);
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = worker.call(|reply| Cmd::FetchCluster { reply }).await;
            let _ = this.update_in(cx, |app, window, cx| app.on_cluster_loaded(id, result, window, cx));
        })
        .detach();
    }

    fn refresh_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(id) = self.session().connection.clone() {
            let selected = self.session().topic.as_ref().map(|t| t.name.clone());
            self.connect(id, window, cx);
            self.reselect_after_refresh = selected;
        }
    }

    fn on_cluster_loaded(
        &mut self,
        id: String,
        result: anyhow::Result<ClusterInfo>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(cluster) => {
                crate::startup::trace(&format!(
                    "cluster loaded: {} brokers, {} topics",
                    cluster.brokers.len(),
                    cluster.topics.len()
                ));
                let cluster = Rc::new(cluster);
                self.states.insert(id.clone(), ConnState::Connected(cluster.clone()));
                if self.session().connection.as_deref() == Some(&id) {
                    let topics: Vec<Arc<TopicInfo>> = cluster.topics.iter().cloned().map(Arc::new).collect();
                    self.set_topics(topics, cx);
                    match self.reselect_after_refresh.take() {
                        Some(name) => self.select_topic_by_name(&name, cx),
                        None => self.restore_view(&id, cx),
                    }
                    self.sync_messages_view(window, cx);
                    if let Ok(name) = std::env::var("KAFKAMITTER_DEV_TOPIC") {
                        self.select_topic_by_name(&name, cx);
                        self.sync_messages_view(window, cx);
                        if let Ok(tab) = std::env::var("KAFKAMITTER_DEV_TAB") {
                            self.session_mut().tab = match tab.as_str() {
                                "produce" => MainTab::Produce,
                                "consumers" => MainTab::Consumers,
                                _ => MainTab::Messages,
                            };
                            if self.session().tab == MainTab::Consumers {
                                self.consumers().update(cx, |view, cx| {
                                    view.set_include_inactive(true);
                                    view.ensure_loaded(window, cx);
                                });
                            }
                        }
                        if let Ok(query) = std::env::var("KAFKAMITTER_DEV_SEARCH") {
                            self.messages().update(cx, |view, cx| view.set_search(&query, window, cx));
                        }
                        if std::env::var_os("KAFKAMITTER_DEV_PRODUCE").is_some() {
                            self.produce().update(cx, |view, cx| view.send_test_message(window, cx));
                        }
                        if std::env::var_os("KAFKAMITTER_DEV_CONSUME").is_some() && !self.settings.auto_consume_on_select {
                            self.messages().update(cx, |view, cx| view.start_default(window, cx));
                        }
                    }
                    if std::env::var_os("KAFKAMITTER_DEV_RETRY").is_some() && !self.dev_retried {
                        self.dev_retried = true;
                        self.reconnect(id.clone(), window, cx);
                        return;
                    }
                    self.run_dev_tab_test(window, cx);
                    self.run_dev_switch_test(&id, window, cx);
                }
            }
            Err(err) => {
                crate::startup::trace(&format!("connection failed: {err}"));
                if std::env::var_os("KAFKAMITTER_DEV_RETRY").is_some() && !self.dev_retried {
                    self.dev_retried = true;
                    self.states.insert(id.clone(), ConnState::Failed(err.to_string()));
                    self.reconnect(id.clone(), window, cx);
                    return;
                }
                self.states.insert(id.clone(), ConnState::Failed(err.to_string()));
                self.kafka.disconnect(&id);
                window.push_notification(Notification::error(format!("Connection failed: {err}")), cx);
            }
        }
        cx.notify();
    }

    /// Switches to another connection and back, to prove the view is remembered.
    /// Drives the tab actions so their effects show up in the startup trace.
    fn run_dev_tab_test(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if std::env::var_os("KAFKAMITTER_DEV_TABTEST").is_none() {
            return;
        }
        let mode = std::env::var("KAFKAMITTER_DEV_TABTEST").unwrap_or_default();
        crate::startup::trace(&format!("tabtest: start with {} tab(s)", self.sessions.len()));
        self.on_new_tab(&NewTab, window, cx);
        crate::startup::trace(&format!(
            "tabtest: new tab shows connection={} topic={}",
            self.session().connection.as_deref().unwrap_or("-"),
            self.session().topic.as_ref().map_or("-", |t| t.name.as_str())
        ));
        if mode == "new" {
            crate::startup::trace("tabtest: staying on the new tab");
            return;
        }
        self.select_session(0, cx);
        if mode == "switch" {
            crate::startup::trace("tabtest: switch only, leaving the session running");
            return;
        }
        self.on_duplicate_tab(&DuplicateTab, window, cx);
        while self.sessions.len() < MAX_TABS + 2 {
            let before = self.sessions.len();
            self.on_new_tab(&NewTab, window, cx);
            if self.sessions.len() == before {
                break;
            }
        }
        crate::startup::trace(&format!("tabtest: capped at {} tab(s)", self.sessions.len()));
        self.on_close_tab(&CloseTab, window, cx);
        crate::startup::trace(&format!("tabtest: after close {} tab(s)", self.sessions.len()));
        self.on_close_all_tabs(&CloseAllTabs, window, cx);
        crate::startup::trace(&format!("tabtest: after close all {} tab(s)", self.sessions.len()));
    }

    fn run_dev_switch_test(&mut self, loaded_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(back_id) = self.dev_switch_back.take() {
            // Clear the view on the second connection, so a restored value can only
            // come from the remembered state of the first one.
            self.session_mut().topic = None;
            self.session_mut().tab = MainTab::Messages;
            self.sync_messages_view(window, cx);
            crate::startup::trace("switch test: second connection reset to topic=- tab=Messages");
            self.activate_connection(back_id, window, cx);
            crate::startup::trace(&format!(
                "switch test result: topic={} tab={}",
                self.session().topic.as_ref().map_or("-", |t| t.name.as_str()),
                self.session().tab.label()
            ));
            return;
        }
        let Ok(other) = std::env::var("KAFKAMITTER_DEV_SWITCH") else {
            return;
        };
        let Some(other_id) = self.profiles.iter().find(|p| p.name == other).map(|p| p.id.clone()) else {
            return;
        };
        crate::startup::trace(&format!(
            "switch test: leaving topic={} tab={}",
            self.session().topic.as_ref().map_or("-", |t| t.name.as_str()),
            self.session().tab.label()
        ));
        self.dev_switch_back = Some(loaded_id.to_string());
        self.activate_connection(other_id, window, cx);
    }

    fn set_topics(&mut self, topics: Vec<Arc<TopicInfo>>, cx: &mut Context<Self>) {
        self.topics.update(cx, |list, cx| {
            list.delegate_mut().set_topics(topics);
            cx.notify();
        });
    }

    fn select_topic_by_name(&mut self, name: &str, cx: &mut Context<Self>) {
        let found = self.topics.update(cx, |list, cx| {
            let ix = list.delegate_mut().select_topic(name);
            cx.notify();
            ix.and_then(|ix| list.delegate().topic_at(ix))
        });
        self.session_mut().topic = found;
    }

    fn toggle_internal_topics(&mut self, cx: &mut Context<Self>) {
        self.topics.update(cx, |list, cx| {
            let show = !list.delegate().show_internal();
            list.delegate_mut().set_show_internal(show);
            cx.notify();
        });
        cx.notify();
    }

    fn on_topic_list_event(
        &mut self,
        list: &Entity<ListState<TopicListDelegate>>,
        event: &ListEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let ListEvent::Select(ix) | ListEvent::Confirm(ix) = event {
            self.session_mut().topic = list.read(cx).delegate().topic_at(*ix);
            self.sync_messages_view(window, cx);
            cx.notify();
        }
    }

    fn sync_messages_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let worker = self.session().connection.as_deref().and_then(|id| self.kafka.existing(id));
        let topic = self.session().topic.clone();
        let changed = self
            .messages()
            .update(cx, |view, cx| view.set_topic(worker.clone(), topic.clone(), window, cx));
        self.produce().update(cx, |view, cx| view.set_topic(worker.clone(), topic.clone(), window, cx));
        self.consumers().update(cx, |view, cx| view.set_topic(worker.clone(), topic.clone(), window, cx));
        if changed && worker.is_some() && topic.is_some() && self.settings.auto_consume_on_select {
            self.messages().update(cx, |view, cx| view.start_default(window, cx));
        }
    }

    fn active_worker(&self, window: &mut Window, cx: &mut Context<Self>) -> Option<Rc<WorkerHandle>> {
        let worker = self.session().connection.as_deref().and_then(|id| self.kafka.existing(id));
        if worker.is_none() {
            window.push_notification(Notification::warning("Connect to a cluster first"), cx);
        }
        worker
    }

    fn on_create_topic(&mut self, _: &CreateTopic, window: &mut Window, cx: &mut Context<Self>) {
        if self.active_worker(window, cx).is_some() {
            open_create_topic_dialog(cx.entity().downgrade(), window, cx);
        }
    }

    pub fn create_topic(&mut self, spec: NewTopicSpec, window: &mut Window, cx: &mut Context<Self>) {
        let Some(worker) = self.active_worker(window, cx) else {
            return;
        };
        let name = spec.name.clone();
        cx.spawn_in(window, async move |this, cx| {
            let result = worker.call(|reply| Cmd::CreateTopic { spec, reply }).await;
            let _ = this.update_in(cx, |app, window, cx| match result {
                Ok(()) => {
                    crate::startup::trace(&format!("topic created: {name}"));
                    window.push_notification(Notification::success(format!("Created topic {name}")), cx);
                    app.refresh_active(window, cx);
                    app.reselect_after_refresh = Some(name.clone());
                }
                Err(err) => {
                    crate::startup::trace(&format!("topic create failed: {err}"));
                    window.push_notification(Notification::error(format!("Create topic failed: {err}")), cx);
                }
            });
        })
        .detach();
    }

    fn on_delete_topic(&mut self, _: &DeleteTopic, window: &mut Window, cx: &mut Context<Self>) {
        let delegate = self.topics.read(cx).delegate();
        let Some(topic) = delegate.right_clicked_topic().or_else(|| delegate.selected_topic()) else {
            return;
        };
        let name = topic.name.clone();
        let app = cx.entity().downgrade();
        window.open_alert_dialog(cx, move |alert, _, _| {
            let name = name.clone();
            let app = app.clone();
            alert
                .title("Delete topic")
                .description(format!("Delete topic \"{name}\" and all of its messages? This cannot be undone."))
                .show_cancel(true)
                .on_ok(move |_, window, cx| {
                    let _ = app.update(cx, |app, cx| app.delete_topic(name.clone(), window, cx));
                    true
                })
        });
    }

    pub fn delete_topic(&mut self, name: String, window: &mut Window, cx: &mut Context<Self>) {
        let Some(worker) = self.active_worker(window, cx) else {
            return;
        };
        cx.spawn_in(window, async move |this, cx| {
            let result = worker
                .call(|reply| Cmd::DeleteTopic {
                    name: name.clone(),
                    reply,
                })
                .await;
            let _ = this.update_in(cx, |app, window, cx| match result {
                Ok(()) => {
                    crate::startup::trace(&format!("topic deleted: {name}"));
                    window.push_notification(Notification::success(format!("Deleted topic {name}")), cx);
                    if app.session().topic.as_ref().is_some_and(|t| t.name == name) {
                        app.session_mut().topic = None;
                        app.sync_messages_view(window, cx);
                    }
                    app.refresh_active(window, cx);
                }
                Err(err) => {
                    crate::startup::trace(&format!("topic delete failed: {err}"));
                    window.push_notification(Notification::error(format!("Delete topic failed: {err}")), cx);
                }
            });
        })
        .detach();
    }

    fn active_cluster(&self) -> Option<&Rc<ClusterInfo>> {
        match self.session().connection.as_deref().and_then(|id| self.states.get(id)) {
            Some(ConnState::Connected(cluster)) => Some(cluster),
            _ => None,
        }
    }

    fn render_connection_row(&self, ix: usize, profile: &ConnectionProfile, cx: &mut Context<Self>) -> AnyElement {
        let id = profile.id.clone();
        let is_active = self.session().connection.as_deref() == Some(profile.id.as_str());
        let failed = matches!(self.states.get(&profile.id), Some(ConnState::Failed(_)));
        let (dot, hint) = match self.states.get(&profile.id) {
            Some(ConnState::Connected(cluster)) => (
                cx.theme().green,
                format!("{} brokers, {} topics", cluster.brokers.len(), cluster.topics.len()),
            ),
            Some(ConnState::Connecting) => (cx.theme().yellow, "Connecting".to_string()),
            Some(ConnState::Failed(err)) => (cx.theme().red, err.clone()),
            None => (cx.theme().muted_foreground, profile.bootstrap_servers.clone()),
        };
        let weak = cx.entity().downgrade();
        let click_id = id.clone();
        let retry_button = failed.then(|| {
            let retry_id = id.clone();
            Button::new(("retry-connection", ix))
                .ghost()
                .xsmall()
                .icon(IconName::RotateCw)
                .tooltip("Reconnect")
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.reconnect(retry_id.clone(), window, cx)
                }))
        });
        h_flex()
            .id(SharedString::from(format!("connection-{}", profile.id)))
            .w_full()
            .px_2()
            .py_1p5()
            .gap_2()
            .items_center()
            .rounded(cx.theme().radius)
            .cursor_pointer()
            .hover(|style| style.bg(cx.theme().sidebar_accent))
            .when(is_active, |el| {
                el.bg(cx.theme().sidebar_accent)
                    .text_color(cx.theme().sidebar_accent_foreground)
            })
            .on_click(cx.listener(move |this, _, window, cx| {
                this.activate_connection(click_id.clone(), window, cx);
            }))
            .context_menu(move |menu, _, _| {
                let edit = weak.clone();
                let edit_id = id.clone();
                let disconnect = weak.clone();
                let disconnect_id = id.clone();
                let remove = weak.clone();
                let remove_id = id.clone();
                menu.item(PopupMenuItem::new("Edit").on_click(move |_, window, cx| {
                    let _ = edit.update(cx, |app, cx| app.edit_profile(&edit_id, window, cx));
                }))
                .item(PopupMenuItem::new("Disconnect").on_click(move |_, window, cx| {
                    let _ = disconnect.update(cx, |app, cx| app.disconnect(&disconnect_id, window, cx));
                }))
                .separator()
                .item(PopupMenuItem::new("Remove").on_click(move |_, window, cx| {
                    let _ = remove.update(cx, |app, cx| app.remove_profile(&remove_id, window, cx));
                }))
            })
            .child(div().size_2().flex_shrink_0().rounded_full().bg(dot))
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .child(div().text_sm().truncate().child(profile.name.clone()))
                    .child(
                        div()
                            .text_xs()
                            .truncate()
                            .text_color(cx.theme().muted_foreground)
                            .child(hint),
                    ),
            )
            .children(retry_button)
            .child({
                let weak = cx.entity().downgrade();
                let menu_id = profile.id.clone();
                Button::new(("connection-menu", ix))
                    .ghost()
                    .xsmall()
                    .icon(IconName::Ellipsis)
                    .dropdown_menu(move |menu, _, _| {
                        let reconnect = weak.clone();
                        let reconnect_id = menu_id.clone();
                        let edit = weak.clone();
                        let edit_id = menu_id.clone();
                        let disconnect = weak.clone();
                        let disconnect_id = menu_id.clone();
                        let remove = weak.clone();
                        let remove_id = menu_id.clone();
                        menu.item(PopupMenuItem::new("Reconnect").on_click(move |_, window, cx| {
                            let _ = reconnect
                                .update(cx, |app, cx| app.reconnect(reconnect_id.clone(), window, cx));
                        }))
                        .item(PopupMenuItem::new("Edit or rename…").on_click(move |_, window, cx| {
                            let _ = edit.update(cx, |app, cx| app.edit_profile(&edit_id, window, cx));
                        }))
                        .item(PopupMenuItem::new("Disconnect").on_click(move |_, window, cx| {
                            let _ = disconnect.update(cx, |app, cx| app.disconnect(&disconnect_id, window, cx));
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Remove").on_click(move |_, window, cx| {
                            let _ = remove.update(cx, |app, cx| app.remove_profile(&remove_id, window, cx));
                        }))
                    })
            })
            .into_any_element()
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let show_internal = self.topics.read(cx).delegate().show_internal();
        let profiles = self.profiles.clone();
        let connection_rows: Vec<AnyElement> = profiles
            .iter()
            .enumerate()
            .map(|(ix, p)| self.render_connection_row(ix, p, cx))
            .collect();
        let section_title = |title: &'static str, cx: &Context<Self>| {
            div()
                .text_xs()
                .font_semibold()
                .text_color(cx.theme().muted_foreground)
                .child(title)
        };
        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .text_color(cx.theme().sidebar_foreground)
            .border_r_1()
            .border_color(cx.theme().sidebar_border)
            .child(
                h_flex()
                    .px_3()
                    .py_2()
                    .justify_between()
                    .items_center()
                    .child(section_title("CONNECTIONS", cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("import-connection")
                                    .ghost()
                                    .xsmall()
                                    .icon(IconName::FolderOpen)
                                    .tooltip("Import a Kafka .properties file")
                                    .on_click(cx.listener(|this, _, window, cx| this.import_properties(window, cx))),
                            )
                            .child(
                                Button::new("add-connection")
                                    .ghost()
                                    .xsmall()
                                    .icon(IconName::Plus)
                                    .tooltip("New connection")
                                    .on_click(cx.listener(|this, _, window, cx| this.open_new_connection(window, cx))),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .px_2()
                    .gap_0p5()
                    .children(connection_rows)
                    .when(self.profiles.is_empty(), |el| {
                        el.child(
                            div()
                                .px_2()
                                .py_1()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("No connections yet"),
                        )
                    }),
            )
            .child(div().mt_2().border_t_1().border_color(cx.theme().sidebar_border))
            .child(
                h_flex()
                    .px_3()
                    .py_2()
                    .justify_between()
                    .items_center()
                    .child(section_title("TOPICS", cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("create-topic")
                                    .ghost()
                                    .xsmall()
                                    .icon(IconName::Plus)
                                    .tooltip("Create topic")
                                    .on_click(cx.listener(|this, _, window, cx| this.on_create_topic(&CreateTopic, window, cx))),
                            )
                            .child(
                                Button::new("toggle-internal")
                                    .ghost()
                                    .xsmall()
                                    .icon(if show_internal { IconName::Eye } else { IconName::EyeOff })
                                    .tooltip("Show internal topics")
                                    .on_click(cx.listener(|this, _, _, cx| this.toggle_internal_topics(cx))),
                            )
                            .child(
                                Button::new("refresh-topics")
                                    .ghost()
                                    .xsmall()
                                    .icon(IconName::RotateCw)
                                    .tooltip("Refresh")
                                    .on_click(cx.listener(|this, _, window, cx| this.refresh_active(window, cx))),
                            ),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .px_1()
                    .pb_1()
                    .child(
                        List::new(&self.topics)
                            .size_full()
                            .with_size(Size::Small)
                            .search_placeholder("Filter topics"),
                    ),
            )
            .into_any_element()
    }

    fn render_session_tabs(&self, cx: &mut Context<Self>) -> AnyElement {
        let full = self.sessions.len() >= MAX_TABS;
        let radius = cx.theme().radius;
        let tabs: Vec<AnyElement> = self
            .sessions
            .iter()
            .enumerate()
            .map(|(ix, session)| {
                let active = ix == self.current;
                let key = session.id as usize;
                h_flex()
                    .id(("session-tab", key))
                    .flex_none()
                    .max_w(px(200.))
                    .px_2p5()
                    .py_1p5()
                    .gap_1p5()
                    .items_center()
                    .rounded(radius)
                    .cursor_pointer()
                    .text_sm()
                    .when(active, |el| {
                        el.bg(cx.theme().tab_active)
                            .text_color(cx.theme().tab_active_foreground)
                    })
                    .when(!active, |el| {
                        el.text_color(cx.theme().tab_foreground)
                            .hover(|style| style.bg(cx.theme().tab))
                    })
                    .on_click(cx.listener(move |this, _, _, cx| this.select_session(ix, cx)))
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .child(self.session_label(session)),
                    )
                    .child(
                        Button::new(("close-tab", key))
                            .ghost()
                            .xsmall()
                            .icon(IconName::Close)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.close_session(ix, window, cx)
                            })),
                    )
                    .context_menu({
                        let app = cx.entity().downgrade();
                        move |menu, _, _| {
                            let duplicate = app.clone();
                            let close = app.clone();
                            let close_all = app.clone();
                            menu.item(PopupMenuItem::new("Duplicate tab").on_click(
                                move |_, window, cx| {
                                    let _ = duplicate
                                        .update(cx, |app, cx| app.duplicate_session(ix, window, cx));
                                },
                            ))
                            .item(PopupMenuItem::new("Close tab").on_click(move |_, window, cx| {
                                let _ = close.update(cx, |app, cx| app.close_session(ix, window, cx));
                            }))
                            .separator()
                            .item(PopupMenuItem::new("Close all tabs").on_click(
                                move |_, window, cx| {
                                    let _ = close_all.update(cx, |app, cx| {
                                        app.on_close_all_tabs(&CloseAllTabs, window, cx)
                                    });
                                },
                            ))
                        }
                    })
                    .into_any_element()
            })
            .collect();

        h_flex()
            .w_full()
            .flex_none()
            .px_2()
            .py_1()
            .gap_0p5()
            .items_center()
            .overflow_x_hidden()
            .bg(cx.theme().tab_bar)
            .border_b_1()
            .border_color(cx.theme().border)
            .children(tabs)
            .child(
                div()
                    .id("new-tab")
                    .flex_none()
                    .px_2p5()
                    .py_1p5()
                    .rounded(radius)
                    .text_sm()
                    .when(full, |el| el.text_color(cx.theme().muted_foreground))
                    .when(!full, |el| {
                        el.cursor_pointer()
                            .text_color(cx.theme().tab_foreground)
                            .hover(|style| style.bg(cx.theme().tab))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.on_new_tab(&NewTab, window, cx)
                            }))
                    })
                    .child("+"),
            )
            .into_any_element()
    }

    fn render_main(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .flex_1()
            .min_w_0()
            .h_full()
            .child(self.render_session_tabs(cx))
            .child(self.render_session_content(window, cx))
            .into_any_element()
    }

    fn render_connection_error(&self, id: &str, error: String, cx: &mut Context<Self>) -> AnyElement {
        let name = self
            .profiles
            .iter()
            .find(|p| p.id == id)
            .map_or_else(|| id.to_string(), |p| p.name.clone());
        let retry_id = id.to_string();
        let edit_id = id.to_string();
        v_flex()
            .flex_1()
            .min_w_0()
            .h_full()
            .items_center()
            .justify_center()
            .gap_3()
            .p_6()
            .child(
                div()
                    .text_lg()
                    .font_semibold()
                    .child(format!("Cannot reach {name}")),
            )
            .child(
                div()
                    .max_w(px(560.))
                    .text_sm()
                    .text_center()
                    .text_color(cx.theme().muted_foreground)
                    .child(error),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("retry-connection-main")
                            .primary()
                            .icon(IconName::RotateCw)
                            .label("Retry")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.reconnect(retry_id.clone(), window, cx)
                            })),
                    )
                    .child(
                        Button::new("edit-connection-main")
                            .outline()
                            .label("Edit connection…")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.edit_profile(&edit_id, window, cx)
                            })),
                    ),
            )
            .into_any_element()
    }

    fn render_session_content(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        if let Some(id) = self.session().connection.clone() {
            match self.states.get(&id) {
                Some(ConnState::Failed(error)) => {
                    let error = error.clone();
                    return self.render_connection_error(&id, error, cx);
                }
                Some(ConnState::Connecting) => {
                    return v_flex()
                        .flex_1()
                        .min_w_0()
                        .h_full()
                        .items_center()
                        .justify_center()
                        .child(
                            div()
                                .text_color(cx.theme().muted_foreground)
                                .child("Connecting…"),
                        )
                        .into_any_element();
                }
                _ => {}
            }
        }
        let Some(topic) = self.session().topic.clone() else {
            let hint = if self.session().connection.is_some() {
                "Select a topic"
            } else if self.profiles.is_empty() {
                "Add a connection to start"
            } else {
                "Select a connection from the left"
            };
            return v_flex()
                .flex_1()
                .min_w_0()
                .h_full()
                .items_center()
                .justify_center()
                .child(div().text_color(cx.theme().muted_foreground).child(hint))
                .into_any_element();
        };
        let replication = topic.partitions.first().map_or(0, |p| p.replicas.len());
        let tab_ix = MainTab::ALL.iter().position(|t| *t == self.session().tab).unwrap_or(0);
        let content: AnyElement = match self.session().tab {
            MainTab::Messages => self.messages().into_any_element(),
            MainTab::Produce => self.produce().into_any_element(),
            MainTab::Consumers => {
                self.consumers().update(cx, |view, cx| view.ensure_loaded(window, cx));
                self.consumers().into_any_element()
            }
        };
        v_flex()
            .flex_1()
            .min_w_0()
            .h_full()
            .child(
                h_flex()
                    .px_4()
                    .py_3()
                    .gap_3()
                    .items_baseline()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(div().text_lg().font_semibold().child(topic.name.clone()))
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("{} partitions, replication {replication}", topic.partitions.len())),
                    ),
            )
            .child(
                TabBar::new("main-tabs")
                    .underline()
                    .px_3()
                    .selected_index(tab_ix)
                    .on_click(cx.listener(|this, ix: &usize, _, cx| {
                        this.session_mut().tab = MainTab::ALL[(*ix).min(MainTab::ALL.len() - 1)];
                        cx.notify();
                    }))
                    .children(MainTab::ALL.iter().map(|t| Tab::new().label(t.label()))),
            )
            .child(div().flex_1().min_h_0().w_full().child(content))
            .into_any_element()
    }
}

impl Render for KafkamitterApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.first_render_traced {
            self.first_render_traced = true;
            crate::startup::trace("first render");
        }
        let sheet_layer = Root::render_sheet_layer(window, cx);
        let dialog_layer = Root::render_dialog_layer(window, cx);
        let notification_layer = Root::render_notification_layer(window, cx);
        let summary = self.active_cluster().map(|cluster| {
            format!(
                "{} · {} brokers",
                self.session().connection
                    .as_deref()
                    .and_then(|id| self.profiles.iter().find(|p| p.id == id))
                    .map_or("", |p| p.name.as_str()),
                cluster.brokers.len()
            )
        });
        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(Self::on_create_topic))
            .on_action(cx.listener(Self::on_delete_topic))
            .on_action(cx.listener(Self::on_open_settings))
            .on_action(cx.listener(Self::on_go_to_top))
            .on_action(cx.listener(Self::on_go_to_bottom))
            .on_action(cx.listener(Self::on_focus_message_value))
            .on_action(cx.listener(Self::on_focus_search))
            .on_action(cx.listener(Self::on_next_connection))
            .on_action(cx.listener(Self::on_previous_connection))
            .on_action(cx.listener(Self::on_switch_connection))
            .on_action(cx.listener(Self::on_edit_active_connection))
            .on_action(cx.listener(Self::on_consume_selected))
            .on_action(cx.listener(Self::on_stop_consume))
            .on_action(cx.listener(Self::on_new_tab))
            .on_action(cx.listener(Self::on_duplicate_tab))
            .on_action(cx.listener(Self::on_close_tab))
            .on_action(cx.listener(Self::on_close_all_tabs))

            .child(
                TitleBar::new().child(
                    h_flex()
                        .w_full()
                        .pr_2()
                        .justify_between()
                        .child(div().font_semibold().child("Kafkamitter"))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(summary.unwrap_or_default()),
                        ),
                ),
            )
            .child(
                div().flex_1().min_h_0().w_full().child(
                    h_resizable("main-split")
                        .with_state(&self.split)
                        .child(
                            resizable_panel()
                                .size(px(280.))
                                .size_range(px(200.)..px(600.))
                                .child(self.render_sidebar(cx)),
                        )
                        .child(resizable_panel().child(self.render_main(window, cx))),
                ),
            )
            .children(sheet_layer)
            .children(dialog_layer)
            .children(notification_layer)
    }
}
