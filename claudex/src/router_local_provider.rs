//! Self-hosted model selection stays on genuine Codex; facade routing is unchanged.
use super::*;
use crate::local_provider::{Config, PROVIDER};

pub(super) struct Replacement {
    public_id: String,
    old_native_id: String,
    request: Value,
    fresh: FreshThread,
    pub(super) started: Option<Value>,
}

impl Router {
    pub(super) fn refresh_local_provider_routing(&mut self) {
        if let Some(local_provider) = &mut self.local_provider
            && let Err(e) = local_provider.refresh_threads()
        {
            note!("Could not refresh shared self-hosted thread routing: {e:#}");
        }
    }
    pub(super) fn local_provider_model(&self, id: &str) -> bool {
        self.local_provider
            .as_ref()
            .is_some_and(|local_provider| local_provider.config.model(id).is_some())
            && !self.local_provider_conflicts.contains(id)
            && !self.is_acp_model(id)
    }

    pub(super) fn request_local_provider_models(&mut self) {
        let Some(local_provider) = &self.local_provider else {
            return;
        };
        if self.local_provider_refreshing {
            return;
        }
        let Some(events) = self.events.clone() else {
            return;
        };
        let config = local_provider.config.clone();
        self.local_provider_refreshing = true;
        tokio::spawn(async move {
            let result = config.refresh().await.map_err(|e| format!("{e:#}"));
            let _ = events.send(Input::LocalProviderModels(result)).await;
        });
    }

    pub(super) fn local_provider_models_received(&mut self, result: Result<Config, String>) {
        self.local_provider_refreshing = false;
        match (self.local_provider.as_mut(), result) {
            (Some(local_provider), Ok(config)) => match config.write(&local_provider.root) {
                Ok(()) => local_provider.config = config,
                Err(e) => note!("Could not refresh self-hosted model metadata: {e:#}"),
            },
            (_, Err(e)) => {
                note!("self-hosted discovery failed; retaining saved model metadata: {e}")
            }
            _ => {}
        }
        self.finish_merges();
    }

    pub(super) fn apply_local_provider_settings(&self, message: &mut Value) -> Result<()> {
        if !matches!(
            message["method"].as_str(),
            Some("thread/start" | "thread/resume" | "thread/fork")
        ) {
            return Ok(());
        }
        let Some(local_provider) = &self.local_provider else {
            return Ok(());
        };
        let params = &mut message["params"];
        let owned = params["threadId"]
            .as_str()
            .is_some_and(|id| local_provider.owns(id));
        let selected = selected_model(params).is_some_and(|model| self.local_provider_model(model));
        if owned || selected || params["modelProvider"] == PROVIDER {
            local_provider.config.apply(&local_provider.root, params)?;
        }
        Ok(())
    }

    /// The native provider is immutable. Only a prewarm known to have no turns
    /// can be recreated; its public id remains stable for the desktop.
    pub(super) fn route_local_provider_selection(&mut self, thread: &str, message: &Value) -> bool {
        let Some(local_provider) = &self.local_provider else {
            return false;
        };
        let owned = local_provider.owns(thread);
        let method = message["method"].as_str().unwrap_or("");
        if matches!(method, "thread/resume" | "thread/fork")
            && !self.local_provider_checked.contains(thread)
            && !self.fresh.contains_key(thread)
            && !owned
        {
            self.block_thread(thread, Block::LocalProvider);
            self.internal_request(
                Side::Codex,
                "thread/read",
                json!({"threadId":thread}),
                Continuation::LocalProviderProbe {
                    thread_id: thread.into(),
                    request: message.clone(),
                },
            );
            return true;
        }
        let Some(model) = selected_model(&message["params"]) else {
            return false;
        };
        // Claude/ACP selections must retain the router's existing adoption path.
        if self.model_side(model) != Some(Side::Codex) {
            return false;
        }
        let selected = self.local_provider_model(model);
        if selected == owned {
            return false;
        }
        if matches!(method, "thread/settings/update" | "turn/start")
            && self.fresh.contains_key(thread)
        {
            let fresh = self.fresh[thread].clone();
            let replacement = Replacement {
                public_id: thread.into(),
                old_native_id: local_provider.native_id(thread).into(),
                request: message.clone(),
                fresh,
                started: None,
            };
            let mut params = replacement.fresh.params.clone();
            params["model"] = json!(model);
            // The saved start payload predates self-hosted provider injection. An explicit
            // provider echoed by the renderer must not pin a GPT selection to a local provider.
            params["modelProvider"] = json!(if selected {
                PROVIDER
            } else {
                &self.codex_provider
            });
            if selected
                && let Err(e) = local_provider
                    .config
                    .apply(&local_provider.root, &mut params)
            {
                self.send_client(error(message["id"].clone(), e.to_string()));
                return true;
            }
            self.block_thread(thread, Block::LocalProvider);
            self.local_provider_replacing += 1;
            self.internal_request(
                Side::Codex,
                "thread/start",
                params,
                Continuation::LocalProviderStart {
                    replacement: Box::new(replacement),
                },
            );
        } else {
            self.send_client(error(message["id"].clone(),
                "This chat keeps its original model provider; start a new chat to switch between self-hosted models and GPT"));
        }
        true
    }

    pub(super) fn local_provider_start_received(
        &mut self,
        mut replacement: Box<Replacement>,
        result: Result<Value, Value>,
    ) {
        match result {
            Ok(result) if result["thread"]["id"].is_string() => {
                let native = result["thread"]["id"].clone();
                replacement.started = Some(result);
                if replacement
                    .fresh
                    .settings
                    .as_object()
                    .is_some_and(|settings| !settings.is_empty())
                {
                    let mut params = replacement.fresh.settings.clone();
                    params["threadId"] = native;
                    params["model"] = json!(selected_model(&replacement.request["params"]));
                    self.internal_request(
                        Side::Codex,
                        "thread/settings/update",
                        params,
                        Continuation::LocalProviderSettings { replacement },
                    );
                } else {
                    self.local_provider_restore_metadata(replacement);
                }
            }
            Ok(_) => self.local_provider_replacement_failed(
                replacement,
                json!({"code":-32603,"message":"Codex returned no replacement thread identity"}),
            ),
            Err(e) => self.local_provider_replacement_failed(replacement, e),
        }
    }

    pub(super) fn local_provider_restore_metadata(&mut self, replacement: Box<Replacement>) {
        let metadata = &replacement.fresh.metadata;
        if metadata.get("projectId").is_some_and(|id| !id.is_null()) {
            let native = &replacement.started.as_ref().unwrap()["thread"]["id"];
            self.internal_request(
                Side::Codex,
                "thread/metadata/update",
                json!({"threadId":native,"projectId":metadata["projectId"]}),
                Continuation::LocalProviderMetadata { replacement },
            );
        } else {
            self.local_provider_replacement_ready(replacement);
        }
    }

    pub(super) fn local_provider_replacement_ready(&mut self, replacement: Box<Replacement>) {
        let result = replacement.started.as_ref().unwrap();
        let native = result["thread"]["id"].as_str().unwrap();
        let provider = result["modelProvider"]
            .as_str()
            .or_else(|| result["thread"]["modelProvider"].as_str())
            .unwrap_or(&self.codex_provider);
        let model = selected_model(&replacement.request["params"]).unwrap();
        if let Err(e) = self.local_provider.as_mut().unwrap().replace(
            &replacement.public_id,
            &replacement.old_native_id,
            native,
            model,
            provider,
        ) {
            self.local_provider_replacement_failed(
                replacement,
                json!({"code":-32603,"message":format!("Could not save self-hosted routing: {e:#}")}),
            );
            return;
        }
        let mut fresh = replacement.fresh;
        fresh.metadata = result["thread"].clone();
        self.local_provider
            .as_ref()
            .unwrap()
            .to_public(&mut fresh.metadata);
        self.fresh.insert(replacement.public_id.clone(), fresh);
        self.threads
            .insert(replacement.public_id.clone(), Side::Codex);
        self.local_provider_checked
            .insert(replacement.public_id.clone());
        self.local_provider_discarded
            .insert(replacement.old_native_id.clone());
        // Use the old native id directly; the public id now names its replacement.
        self.local_provider_unsubscribe_native(&replacement.old_native_id);
        self.local_provider_replacing -= 1;
        self.local_provider_flush_notifications();
        self.forward(Side::Codex, replacement.request);
        self.unblock_thread(&replacement.public_id, Block::LocalProvider);
    }

    pub(super) fn local_provider_replacement_failed(
        &mut self,
        replacement: Box<Replacement>,
        failure: Value,
    ) {
        if let Some(started) = &replacement.started
            && let Some(id) = started["thread"]["id"].as_str()
        {
            self.local_provider_discarded.insert(id.to_owned());
            self.local_provider_unsubscribe_native(id);
        }
        self.local_provider_replacing -= 1;
        self.local_provider_flush_notifications();
        self.send_client(json!({"id":replacement.request["id"],"error":failure}));
        self.unblock_thread(&replacement.public_id, Block::LocalProvider);
    }

    fn local_provider_unsubscribe_native(&mut self, native: &str) {
        let id = format!("{INTERNAL_ID}{}", self.next_internal);
        self.next_internal += 1;
        self.internal
            .insert(id.clone(), (Side::Codex, Continuation::Ignore));
        if self
            .codex
            .try_send(json!({"id":id,"method":"thread/unsubscribe","params":{"threadId":native}}))
            .is_err()
        {
            self.internal.remove(&id);
        }
    }

    /// Buffer starts until their native ids are available for durable aliasing.
    /// Other chats keep running while the one prewarm completes its transaction.
    pub(super) fn local_provider_incoming(&mut self, message: &mut Value) -> bool {
        if self.local_provider.is_none() {
            return true;
        }
        let ids = thread_ids(message);
        if ids
            .iter()
            .any(|id| self.local_provider_discarded.contains(id))
        {
            return false;
        }
        if self.local_provider_replacing > 0 && message["method"] == "thread/started" {
            self.local_provider_notifications.push(message.clone());
            return false;
        }
        if self.local_provider_replacing > 0
            && message["method"].is_string()
            && ids.iter().any(|id| {
                self.local_provider_notifications
                    .iter()
                    .any(|m| thread_ids(m).contains(id))
            })
        {
            self.local_provider_notifications.push(message.clone());
            return false;
        }
        self.local_provider.as_ref().unwrap().to_public(message);
        true
    }

    fn local_provider_flush_notifications(&mut self) {
        if self.local_provider_replacing > 0 {
            return;
        }
        for message in std::mem::take(&mut self.local_provider_notifications) {
            self.backend_message(Side::Codex, message);
        }
    }

    pub(super) fn local_provider_probe_received(
        &mut self,
        thread: String,
        request: Value,
        result: Result<Value, Value>,
    ) {
        if let Ok(result) = &result
            && result["thread"]["modelProvider"] == PROVIDER
        {
            let native = self
                .local_provider
                .as_ref()
                .unwrap()
                .native_id(&thread)
                .to_owned();
            let model = result["thread"]["model"].as_str().unwrap_or("");
            if let Err(e) = self
                .local_provider
                .as_mut()
                .unwrap()
                .remember(&thread, &native, model, PROVIDER)
            {
                self.send_client(error(
                    request["id"].clone(),
                    format!("Could not remember self-hosted routing: {e:#}"),
                ));
                self.unblock_thread(&thread, Block::LocalProvider);
                return;
            }
        }
        self.local_provider_checked.insert(thread.clone());
        self.dispatch(request);
        self.unblock_thread(&thread, Block::LocalProvider);
    }

    pub(super) fn remember_local_provider_result(&mut self, forwarded: &Forwarded, result: &Value) {
        if !matches!(
            forwarded.method.as_str(),
            "thread/start" | "thread/resume" | "thread/fork" | "thread/read"
        ) {
            return;
        }
        let Some(local_provider) = &mut self.local_provider else {
            return;
        };
        let Some(public) = result["thread"]["id"].as_str() else {
            return;
        };
        let provider = result["modelProvider"]
            .as_str()
            .or_else(|| result["thread"]["modelProvider"].as_str())
            .unwrap_or("");
        if provider != PROVIDER {
            return;
        }
        let native = local_provider.native_id(public).to_owned();
        let model = result["model"]
            .as_str()
            .or_else(|| result["thread"]["model"].as_str())
            .or_else(|| selected_model(&forwarded.params))
            .unwrap_or("");
        if let Err(e) = local_provider.remember(public, &native, model, provider) {
            note!("Could not remember self-hosted chat: {e:#}");
        }
        self.local_provider_checked.insert(public.into());
    }
}
