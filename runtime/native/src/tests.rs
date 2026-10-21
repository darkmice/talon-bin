use super::*;

fn fixture() -> (u64, Arc<Operation>) {
    let operation = Arc::new(Operation {
        cancel: CancellationToken::new(),
        events: Mutex::new(Queue {
            messages: VecDeque::new(),
            bytes: 0,
        }),
        terminal: Mutex::new(None),
        space: tokio::sync::Notify::new(),
        callbacks: Mutex::new(HashMap::new()),
        cancelled_callbacks: Mutex::new(VecDeque::new()),
        next_callback: AtomicU64::new(1),
        hitl: Arc::new(talon_agent::HitlChannel::new()),
        hitl_pending: AtomicUsize::new(0),
        finished: Mutex::new(true),
        finished_signal: std::sync::Condvar::new(),
    });
    let mut state = state().lock().unwrap();
    let id = next_id(&mut state).unwrap();
    state.operations.insert(id, operation.clone());
    (id, operation)
}
unsafe fn envelope(input: *const u8, len: usize) -> Value {
    let mut out = std::ptr::null_mut();
    assert_eq!(talon_runtime_call_v1(input, len, &mut out), 0);
    assert!(!out.is_null());
    let value = serde_json::from_slice(std::ffi::CStr::from_ptr(out).to_bytes()).unwrap();
    talon_runtime_free_v1(out);
    value
}

#[test]
fn ffi_rejects_malformed_and_oversized_buffers_before_reading() {
    unsafe {
        assert_eq!(
            talon_runtime_call_v1(std::ptr::null(), 0, std::ptr::null_mut()),
            -1
        );
        assert_eq!(envelope(std::ptr::null(), 1)["ok"], false);
        let byte = [b'{'];
        assert_eq!(envelope(byte.as_ptr(), MAX_MESSAGE + 1)["ok"], false);
        assert_eq!(envelope([0xff].as_ptr(), 1)["code"], "invalid_request");
        let request = br#"{"action":"poll","id":18446744073709551615}"#;
        assert_eq!(envelope(request.as_ptr(), request.len())["ok"], false);
    }
}

#[tokio::test]
async fn bounded_queue_applies_backpressure_and_delivers_terminal_after_cancel() {
    let (id, operation) = fixture();
    for ordinal in 0..128 {
        operation
            .emit(json!({"kind":"fixture","ordinal":ordinal}))
            .await
            .unwrap();
    }
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        operation.emit(json!({"kind":"overflow"}))
    )
    .await
    .is_err());
    operation.cancel.cancel();
    assert!(operation.emit(json!({"kind":"overflow"})).await.is_err());
    *operation.terminal.lock().unwrap() = Some(json!({"kind":"done","result":"cancelled"}));
    let mut messages = vec![];
    for _ in 0..9 {
        let batch = dispatch(json!({"action":"poll","id":id})).unwrap();
        messages.extend(batch.as_array().unwrap().iter().cloned());
    }
    assert_eq!(messages.len(), 129);
    assert_eq!(messages.last().unwrap()["kind"], "done");
    dispatch(json!({"action":"close","id":id})).unwrap();
    assert!(dispatch(json!({"action":"poll","id":id})).is_err());
}

#[tokio::test]
async fn callback_responses_are_correlated_single_terminal_and_drop_cancels_host() {
    let (id, operation) = fixture();
    let (callback, mut replies) = operation
        .callback(json!({"method":"fixture"}))
        .await
        .unwrap();
    assert!(dispatch(
        json!({"action":"reply","id":id,"callbackId":callback+1,"done":true,"value":{}})
    )
    .is_err());
    dispatch(json!({"action":"reply","id":id,"callbackId":callback,"done":true,"value":{"approved":true}})).unwrap();
    assert!(operation.receive(&mut replies).await.unwrap().done);
    assert_eq!(
        dispatch(json!({"action":"reply","id":id,"callbackId":callback,"done":true,"value":{}}))
            .unwrap_err()
            .code,
        "callback_closed"
    );
    let (callback, replies) = operation
        .callback(json!({"method":"fixture"}))
        .await
        .unwrap();
    drop(replies);
    let events = dispatch(json!({"action":"poll","id":id})).unwrap();
    assert_eq!(
        events[0],
        json!({"kind":"callback_cancel","callbackId":callback})
    );
    dispatch(json!({"action":"close","id":id})).unwrap();
}
