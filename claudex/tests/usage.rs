use claude_codex_server::usage::Tracker;
use serde_json::json;

#[test]
fn occupancy_tracks_latest_request_and_streaming_output_not_turn_aggregate() {
    let mut t = Tracker::default();
    let start = json!({"type":"stream_event","event":{"type":"message_start","message":{"model":"main","usage":{"input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":20,"output_tokens":0}}}});
    assert_eq!(t.receive(&start).unwrap()["totalTokens"], 130);
    assert_eq!(t.receive(&json!({"type":"stream_event","event":{"type":"message_delta","usage":{"output_tokens":7}}})).unwrap()["totalTokens"],137);
    let assistant = json!({"type":"assistant","message":{"model":"main","usage":{"input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":20,"output_tokens":7}}});
    assert_eq!(t.receive(&assistant).unwrap()["totalTokens"], 137);
    assert_eq!(t.receive(&assistant).unwrap()["totalTokens"], 137);
    assert!(
        t.receive(&json!({"type":"result","usage":{"input_tokens":999999}}))
            .is_none()
    );
    assert!(t.receive(&json!({"type":"assistant","parent_tool_use_id":"child","message":{"model":"child","usage":{"input_tokens":999999}}})).is_none());
    // A post-compaction request can be smaller than its predecessor.
    assert_eq!(t.receive(&json!({"type":"assistant","message":{"model":"main","usage":{"input_tokens":5,"output_tokens":2}}})).unwrap()["totalTokens"],7);
    assert_eq!(t.context_window(&json!({"modelUsage":{"main":{"contextWindow":200000},"child":{"contextWindow":1000000}}})),Some(200000));
}
