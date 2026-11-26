//! Deterministic stand-in for the one-shot Claude reviewer of ACP permission
//! requests. It checks the review invocation and answers from the action:
//! commands containing `rm -rf` are denied unless the user approved that exact
//! action, `slow-review` never answers, and `crash-review` fails.
use serde_json::{Value, json};
use std::io::Read;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| {
        args.iter()
            .position(|arg| arg == name)
            .and_then(|index| args.get(index + 1))
            .cloned()
            .unwrap_or_else(|| panic!("reviewer invoked without {name}: {args:?}"))
    };
    assert!(args.iter().any(|arg| arg == "-p"));
    assert_eq!(flag("--output-format"), "json");
    assert_eq!(flag("--permission-mode"), "dontAsk");
    // Investigation is read-only.
    assert_eq!(flag("--tools"), "Read,Grep,Glob");
    assert_eq!(flag("--allowedTools"), "Read,Grep,Glob");
    assert!(args.iter().any(|arg| arg == "--no-session-persistence"));
    let schema: Value = serde_json::from_str(&flag("--json-schema")).unwrap();
    assert_eq!(schema["required"], json!(["outcome"]));
    assert!(flag("--system-prompt").contains("You are judging one planned coding-agent action."));
    let mut request = String::new();
    std::io::stdin().read_to_string(&mut request).unwrap();
    assert!(request.contains("<transcript>"), "{request}");
    let action = request
        .split("Planned action to assess (JSON):\n")
        .nth(1)
        .expect("planned action");
    let assessment = if action.contains("crash-review") {
        eprintln!("reviewer exploded");
        std::process::exit(2);
    } else if action.contains("slow-review") {
        std::thread::sleep(std::time::Duration::from_secs(60));
        unreachable!()
    } else if action.contains("rm -rf") {
        // The user's approval is reflected in the transcript of later reviews.
        let approved = request
            .split("previously rejected actions")
            .nth(1)
            .is_some_and(|approved| approved.contains("rm -rf"));
        if approved {
            json!({"outcome":"allow","risk_level":"high","user_authorization":"high",
                "rationale":"The user explicitly approved this deletion."})
        } else {
            json!({"outcome":"deny","risk_level":"high","user_authorization":"low",
                "rationale":"Recursive deletion was not requested by the user."})
        }
    } else {
        json!({"outcome":"allow","risk_level":"low","user_authorization":"medium",
            "rationale":"Routine local command."})
    };
    println!(
        "{}",
        json!({"type":"result","subtype":"success","is_error":false,"result":assessment.to_string(),
            "structured_output":assessment})
    );
}
