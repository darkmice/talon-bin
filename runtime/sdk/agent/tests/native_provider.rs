//! Loopback HTTP fixture exercises the real binary provider, not a provider mock.
use futures::StreamExt;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    time::Duration,
};
use talon_llm::{
    config::{ProviderConfig, ProviderProtocol},
    provider::openai_compat::{OpenAICompatibleProvider, TransportObservation},
    CompletionRequest, LanguageModelProvider, LlmError, StreamEvent,
};
use tokio_util::sync::CancellationToken;

fn fixture(status: u16, body: String) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut input = Vec::new();
        let mut chunk = [0; 4096];
        loop {
            let n = socket.read(&mut chunk).unwrap();
            assert!(n > 0);
            input.extend_from_slice(&chunk[..n]);
            assert!(input.len() < 128 * 1024);
            if let Some(end) = input.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&input[..end]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if input.len() >= end + 4 + length {
                    assert!(headers.contains("authorization: bearer fixture-only"));
                    break;
                }
            }
        }
        let content = if status == 200 {
            "text/event-stream"
        } else {
            "application/json"
        };
        let headers=format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
        socket.write_all(headers.as_bytes()).unwrap();
        for chunk in body.as_bytes().chunks(13) {
            socket.write_all(chunk).unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    (format!("http://{address}/v1"), server)
}
fn config(base: String) -> ProviderConfig {
    ProviderConfig {
        id: "binary-loopback".into(),
        display_name: "Loopback fixture".into(),
        api_base: base,
        api_key: "fixture-only".into(),
        models: vec!["fixture".into()],
        protocol: ProviderProtocol::OpenAiCompatible,
        enabled: true,
        model_overrides: Default::default(),
        compat_hints: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_native_provider_preserves_stream_transport_http_errors_and_file_policy() {
    let body="data: {\"id\":\"fixture\",\"model\":\"fixture\",\"choices\":[{\"delta\":{\"content\":\"中文\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_string();
    let (base, server) = fixture(200, body);
    let observations = Arc::new(Mutex::new(Vec::new()));
    let captured = observations.clone();
    let provider = OpenAICompatibleProvider::new(config(base), vec![])
        .with_transport_observer(Arc::new(move |event| captured.lock().unwrap().push(event)));
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        provider.complete(
            CompletionRequest::new("fixture", "hello"),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let mut text = String::new();
    let mut ended = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
    {
        match event {
            StreamEvent::TextDelta { text: delta } => text.push_str(&delta),
            StreamEvent::StreamEnd { .. } => ended = true,
            StreamEvent::StreamError { error, .. } => panic!("loopback stream error: {error}"),
            _ => {}
        }
    }
    server.join().unwrap();
    assert_eq!(text, "中文");
    assert!(ended);
    let observations = observations.lock().unwrap();
    assert!(matches!(
        observations.first(),
        Some(TransportObservation::RequestStart)
    ));
    assert!(observations.iter().any(|event| matches!(
        event,
        TransportObservation::ResponseHeaders { event_stream: true }
    )));
    assert!(observations
        .iter()
        .any(|event| matches!(event,TransportObservation::BodyChunk{bytes} if *bytes>0)));
    drop(observations);
    let (base, server) = fixture(
        401,
        "{\"error\":{\"message\":\"fixture unauthorized\"}}".into(),
    );
    let provider = OpenAICompatibleProvider::new(config(base), vec![]);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        provider.complete(
            CompletionRequest::new("fixture", "hello"),
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(LlmError::ApiError { status: 401, .. })
    ));
    server.join().unwrap();
    assert!(OpenAICompatibleProvider::count_tokens("gpt-4", "中文 abc").unwrap() > 0);

    use talon_agent_sdk::{
        security::FileAccessPolicy,
        tool::{builtin::BuiltinToolExecutor, ToolExecutor},
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("sample.txt"), "中文").unwrap();
    std::fs::write(root.join(".env"), "fixture-only").unwrap();
    std::fs::write(directory.path().join("outside.txt"), "outside").unwrap();
    let tools = BuiltinToolExecutor::new(Some(
        FileAccessPolicy::new(root.clone()).with_credential_protection(),
    ));
    for (path, allowed) in [
        (root.join("sample.txt"), true),
        (root.join(".env"), false),
        (directory.path().join("outside.txt"), false),
    ] {
        let result = tools
            .execute("read_file", &serde_json::json!({"path":path}))
            .await;
        assert_eq!(!result.is_error, allowed);
        if allowed {
            assert!(result.content.contains("中文"));
        }
    }
}
