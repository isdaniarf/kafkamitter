mod app;
mod cli;
mod startup;
mod ui;

use kafkamitter::{kafka, model};

use gpui::*;
use gpui_component::{Root, TitleBar};

use app::{
    CloseAllTabs, CloseTab, ConsumeSelected, DuplicateTab, EditActiveConnection, FocusMessageValue,
    GoToBottom, GoToTop, NewTab, NextConnection,
    FocusSearch, OpenSettings, PreviousConnection, Quit, StopConsume, SwitchConnection,
};

fn key_bindings() -> Vec<KeyBinding> {
    let mut bindings = vec![
        KeyBinding::new("cmd-q", Quit, None),
        KeyBinding::new("cmd-,", OpenSettings, None),
        KeyBinding::new("cmd-up", GoToTop, Some("!Input && !List")),
        KeyBinding::new("cmd-down", GoToBottom, Some("!Input && !List")),
        KeyBinding::new("enter", FocusMessageValue, Some("!Input && !List")),
        KeyBinding::new("cmd-f", FocusSearch, None),
        KeyBinding::new("cmd-shift-]", NextConnection, None),
        KeyBinding::new("cmd-shift-[", PreviousConnection, None),
        KeyBinding::new("cmd-e", EditActiveConnection, None),
        KeyBinding::new("cmd-r", ConsumeSelected, None),
        KeyBinding::new("cmd-.", StopConsume, None),
        KeyBinding::new("cmd-t", NewTab, None),
        KeyBinding::new("cmd-d", DuplicateTab, None),
        KeyBinding::new("cmd-w", CloseTab, None),
        KeyBinding::new("cmd-shift-w", CloseAllTabs, None),
    ];
    for index in 0..9 {
        bindings.push(KeyBinding::new(
            &format!("cmd-{}", index + 1),
            SwitchConnection { index },
            None,
        ));
    }
    bindings
}

fn menus() -> Vec<Menu> {
    vec![
        Menu {
            name: "Kafkamitter".into(),
            items: vec![
                MenuItem::action("Settings…", OpenSettings),
                MenuItem::Separator,
                MenuItem::action("Quit", Quit),
            ],
            disabled: false,
        },
        Menu {
            name: "Tab".into(),
            items: vec![
                MenuItem::action("New tab", NewTab),
                MenuItem::action("Duplicate tab", DuplicateTab),
                MenuItem::Separator,
                MenuItem::action("Close tab", CloseTab),
                MenuItem::action("Close all tabs", CloseAllTabs),
            ],
            disabled: false,
        },
        Menu {
            name: "Connection".into(),
            items: vec![
                MenuItem::action("Next connection", NextConnection),
                MenuItem::action("Previous connection", PreviousConnection),
                MenuItem::Separator,
                MenuItem::action("Edit connection…", EditActiveConnection),
            ],
            disabled: false,
        },
        Menu {
            name: "Topic".into(),
            items: vec![
                MenuItem::action("Consume", ConsumeSelected),
                MenuItem::action("Stop", StopConsume),
                MenuItem::Separator,
                MenuItem::action("Go to the first row", GoToTop),
                MenuItem::action("Go to the last row", GoToBottom),
                MenuItem::action("Find in messages", FocusSearch),
                MenuItem::action("Focus message value", FocusMessageValue),
            ],
            disabled: false,
        },
    ]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(code) = cli::run(&args) {
        std::process::exit(code);
    }
    startup::mark_start();
    let app = gpui_platform::application().with_assets(gpui_kit_assets::Assets);

    app.run(move |cx| {
        startup::trace("run");
        gpui_component::init(cx);
        cx.set_app_identity("dev.fithrantyo.kafkamitter", "Kafkamitter");
        cx.on_action(|_: &Quit, cx: &mut App| cx.quit());
        cx.bind_keys(key_bindings());
        cx.set_menus(menus());
        if std::env::var_os("KAFKAMITTER_DEV_QUIET").is_none() {
            cx.activate(true);
        }

        let (width, height) = std::env::var("KAFKAMITTER_DEV_WINDOW")
            .ok()
            .and_then(|spec| {
                let (w, h) = spec.split_once('x')?;
                Some((w.trim().parse::<f32>().ok()?, h.trim().parse::<f32>().ok()?))
            })
            .unwrap_or((1280., 820.));
        let bounds = Bounds::centered(None, size(px(width), px(height)), cx);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            ..TitleBar::window_options()
        };
        cx.open_window(options, |window, cx| {
            let view = cx.new(|cx| app::KafkamitterApp::new(window, cx));
            cx.new(|cx| Root::new(view, window, cx))
        })
        .expect("failed to open the main window");
        startup::trace("window opened");
    });
}
