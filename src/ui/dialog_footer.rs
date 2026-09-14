use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::dialog::{DialogAction, DialogClose, DialogFooter};

pub fn ok_cancel_footer(ok_label: &'static str) -> impl IntoElement {
    DialogFooter::new()
        .child(
            div()
                .flex_none()
                .child(DialogClose::new().child(Button::new("dialog-cancel").outline().label("Cancel"))),
        )
        .child(
            div()
                .flex_none()
                .child(DialogAction::new().child(Button::new("dialog-ok").primary().label(ok_label))),
        )
}
