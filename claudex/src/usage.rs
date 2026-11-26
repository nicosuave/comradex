//! Current model-request occupancy is distinct from cumulative turn billing.
use serde_json::{Value, json};

#[derive(Default)]
pub struct Tracker {
    pub model: Option<String>,
    usage: Value,
    /// Context window an ACP agent reported with its latest usage update.
    window: Option<u64>,
}

impl Tracker {
    pub fn receive(&mut self, message: &Value) -> Option<Value> {
        if !message["parent_tool_use_id"].is_null() {
            return None;
        }
        let (usage, replace) = match message["type"].as_str() {
            // ACP reports context occupancy rather than per-request usage.
            Some("acp") if message["update"]["sessionUpdate"] == "usage_update" => {
                let update = &message["update"];
                self.window = update["size"].as_u64().or(self.window);
                return Some(crate::protocol::usage(
                    update["used"].as_u64().unwrap_or(0),
                    0,
                    0,
                    0,
                ));
            }
            Some("assistant") => {
                self.model = message["message"]["model"]
                    .as_str()
                    .map(str::to_owned)
                    .or(self.model.take());
                (&message["message"]["usage"], true)
            }
            Some("stream_event") => match message["event"]["type"].as_str() {
                Some("message_start") => {
                    self.model = message["event"]["message"]["model"]
                        .as_str()
                        .map(str::to_owned);
                    (&message["event"]["message"]["usage"], true)
                }
                Some("message_delta") => (&message["event"]["usage"], false),
                _ => return None,
            },
            _ => return None,
        };
        let fields = usage.as_object()?;
        if replace {
            self.usage = json!({});
        }
        for (key, value) in fields {
            if value.is_u64() {
                self.usage[key] = value.clone();
            }
        }
        Some(from_native(&self.usage))
    }

    pub fn context_window(&self, result: &Value) -> Option<u64> {
        if self.window.is_some() {
            return self.window;
        }
        let models = result["modelUsage"].as_object()?;
        if let Some(model) = &self.model {
            return models.get(model)?["contextWindow"].as_u64();
        }
        (models.len() == 1)
            .then(|| models.values().next().unwrap()["contextWindow"].as_u64())
            .flatten()
    }
}

pub fn from_native(usage: &Value) -> Value {
    crate::protocol::usage(
        usage["input_tokens"].as_u64().unwrap_or(0),
        usage["cache_read_input_tokens"].as_u64().unwrap_or(0),
        usage["cache_creation_input_tokens"].as_u64().unwrap_or(0),
        usage["output_tokens"].as_u64().unwrap_or(0),
    )
}
