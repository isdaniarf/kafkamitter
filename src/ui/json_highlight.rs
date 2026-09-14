use std::ops::Range;
use std::rc::Rc;

use gpui::*;
use gpui_component::input::{
    EditorState, FoldRange, HighlightStyleResolver, InputEdit, InputHighlighter, InputHighlighterFactory, Rope,
};

use crate::model::json::{JsonToken, tokenize};

#[derive(Default)]
pub struct JsonHighlighter {
    runs: Vec<(Range<usize>, &'static str)>,
}

fn style_name(token: JsonToken) -> Option<&'static str> {
    match token {
        JsonToken::Key => Some("property"),
        JsonToken::Str => Some("string"),
        JsonToken::Number => Some("number"),
        JsonToken::Boolean => Some("boolean"),
        JsonToken::Null => Some("constant"),
        JsonToken::Punctuation => None,
    }
}

impl InputHighlighter for JsonHighlighter {
    fn language(&self) -> SharedString {
        "json".into()
    }

    fn update(
        &mut self,
        _edit: Option<InputEdit>,
        text: &Rope,
        _folding: bool,
        _window: &mut Window,
        _cx: &mut Context<EditorState>,
    ) {
        let text = text.to_string();
        self.runs = tokenize(&text)
            .into_iter()
            .filter_map(|(range, token)| style_name(token).map(|name| (range, name)))
            .collect();
    }

    fn styles(
        &self,
        range: &Range<usize>,
        resolver: &dyn HighlightStyleResolver,
    ) -> Vec<(Range<usize>, HighlightStyle)> {
        let mut out = Vec::new();
        let mut cursor = range.start;
        let first = self.runs.partition_point(|(run, _)| run.end <= range.start);
        for (run, name) in &self.runs[first..] {
            if run.start >= range.end {
                break;
            }
            let start = run.start.max(range.start);
            let end = run.end.min(range.end);
            if start > cursor {
                out.push((cursor..start, HighlightStyle::default()));
            }
            out.push((start..end, resolver.style(name).unwrap_or_default()));
            cursor = end;
        }
        if cursor < range.end {
            out.push((cursor..range.end, HighlightStyle::default()));
        }
        out
    }

    fn fold_ranges(&self, _text: &Rope) -> Vec<FoldRange> {
        Vec::new()
    }
}

pub fn factory() -> InputHighlighterFactory {
    Rc::new(|language: &str| {
        if language.eq_ignore_ascii_case("json") {
            Some(Box::new(JsonHighlighter::default()) as Box<dyn InputHighlighter>)
        } else {
            None
        }
    })
}
