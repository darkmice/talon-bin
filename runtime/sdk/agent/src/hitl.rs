//! HITL admission is synchronous once bound: native close/send is the authority.
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use talon_runtime_transport::{Error, Operation};
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum HitlCommand {
    Pause,
    Resume,
    Abort { reason: String },
    Inject { message: String },
    UpdateLimits { new_limits: crate::ExecutionLimits },
}
#[derive(Default)]
struct State {
    queue: VecDeque<HitlCommand>,
    binding: Option<Arc<Operation>>,
    closed: bool,
}
#[derive(Default)]
pub struct HitlChannel {
    state: Mutex<State>,
    injections: AtomicUsize,
    notify: tokio::sync::Notify,
}
pub struct HitlSender<'a> {
    channel: &'a HitlChannel,
}
pub(crate) struct Binding {
    channel: Arc<HitlChannel>,
}
impl Drop for Binding {
    fn drop(&mut self) {
        let mut state = self.channel.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        state.binding.take();
        state.queue.clear();
        self.channel.injections.store(0, Ordering::Release);
    }
}
impl HitlChannel {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn sender(&self) -> HitlSender<'_> {
        HitlSender { channel: self }
    }
    pub fn has_pending_injection(&self) -> bool {
        self.injections.load(Ordering::Acquire) > 0
    }
    pub async fn wait_for_injection(&self) {
        loop {
            let notification = self.notify.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if self.has_pending_injection() {
                return;
            }
            notification.await;
        }
    }
    pub(crate) fn bind(self: &Arc<Self>, operation: Arc<Operation>) -> Result<Binding, Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed || state.binding.is_some() {
            return Err(talon_runtime_transport::error(
                "HITL channel is closed or already bound",
            ));
        }
        while let Some(command) = state.queue.pop_front() {
            if let Err(error) = operation.control("hitl", json!({"command":command})) {
                state.closed = true;
                state.queue.clear();
                self.injections.store(0, Ordering::Release);
                return Err(error);
            }
        }
        state.binding = Some(operation);
        Ok(Binding {
            channel: self.clone(),
        })
    }
    pub(crate) fn applied(&self) {
        let _gate = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let _ = self
            .injections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
    }
}
impl HitlSender<'_> {
    pub fn send(
        &self,
        command: HitlCommand,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<HitlCommand>> {
        let mut state = self.channel.state.lock().unwrap_or_else(|e| e.into_inner());
        let injecting = matches!(&command, HitlCommand::Inject { .. });
        if state.closed
            || state.queue.len() >= 128
            || (injecting && self.channel.injections.load(Ordering::Acquire) >= 128)
        {
            return Err(tokio::sync::mpsc::error::SendError(command));
        }
        if injecting {
            self.channel.injections.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(operation) = &state.binding {
            if operation
                .control("hitl", json!({"command":command}))
                .is_err()
            {
                if injecting {
                    self.channel.injections.fetch_sub(1, Ordering::AcqRel);
                }
                return Err(tokio::sync::mpsc::error::SendError(command));
            }
        } else {
            state.queue.push_back(command);
        }
        drop(state);
        self.channel.notify.notify_waiters();
        Ok(())
    }
}
