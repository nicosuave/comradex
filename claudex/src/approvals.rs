//! Scoped facade approvals; native permission suggestions are intentionally not applied.
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Approval for the identical tool invocation in one thread and working directory.
/// The owning thread stores this value; it must not be copied when forking.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionGrant {
    pub tool: String,
    pub input: Value,
    pub cwd: String,
}

impl SessionGrant {
    pub fn new(tool: &str, input: &Value, cwd: &str) -> Self {
        Self {
            tool: tool.into(),
            input: input.clone(),
            cwd: cwd.into(),
        }
    }

    pub fn permits(&self, tool: &str, input: &Value, cwd: &str, approval_policy: &str) -> bool {
        approval_policy != "never"
            && tool != "AskUserQuestion"
            && self.tool == tool
            && self.input == *input
            && self.cwd == cwd
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Once,
    Session,
    Deny,
    Cancel,
}

/// A native file permission response authorizes only the pending exact read.
/// Do not persist its profile as a broader native filesystem permission rule.
pub fn file_read_decision(result: &Value, requested: &Value) -> Decision {
    let Some(path) = requested["fileSystem"]["read"][0].as_str() else {
        return Decision::Deny;
    };
    let granted = &result["permissions"]["fileSystem"];
    let allowed = granted["read"]
        .as_array()
        .is_some_and(|paths| paths.iter().any(|p| p == path))
        || granted["entries"].as_array().is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry["access"] == "read"
                    && entry["path"]["type"] == "path"
                    && entry["path"]["path"] == path
            })
        });
    if !allowed {
        return Decision::Deny;
    }
    match result["scope"].as_str() {
        Some("session") => Decision::Session,
        None | Some("turn") => Decision::Once,
        _ => Decision::Deny,
    }
}

/// Unknown or structured permission amendments fail closed.
pub fn decision(result: &Value, generic_tool: bool) -> Decision {
    let value = if generic_tool {
        let Some(answers) = result["answers"]["permission"]["answers"].as_array() else {
            return Decision::Deny;
        };
        if answers.len() != 1 {
            return Decision::Deny;
        }
        answers[0].as_str()
    } else {
        result["decision"].as_str()
    };
    match value {
        Some("accept" | "Allow") => Decision::Once,
        Some("acceptForSession" | "Allow for session") => Decision::Session,
        Some("cancel") => Decision::Cancel,
        _ => Decision::Deny,
    }
}

#[derive(Clone, Debug)]
struct Question {
    text: String,
    header: String,
    options: Vec<Value>,
    multiple: bool,
}

/// One native request may require several sequential desktop option pickers.
/// The caller must use a fresh RPC id per request and retain the same native id.
#[derive(Clone, Debug)]
pub struct QuestionFlow {
    input: Value,
    questions: Vec<Question>,
    picker: bool,
    answered: usize,
}

impl QuestionFlow {
    pub fn new(input: &Value) -> Result<Self> {
        let raw = input["questions"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing questions"))?;
        ensure!(!raw.is_empty(), "No questions supplied");
        let mut questions = Vec::new();
        for q in raw {
            let text = q["question"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("Empty question"))?;
            ensure!(
                !questions.iter().any(|q: &Question| q.text == text),
                "Duplicate question text"
            );
            let options = q["options"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("Missing question options"))?;
            ensure!(options.len() >= 2, "Question requires at least two options");
            let mut mapped = Vec::new();
            for option in options {
                let label = option["label"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("Empty option label"))?;
                ensure!(
                    !mapped.iter().any(|v: &Value| v["label"] == label),
                    "Duplicate option label"
                );
                mapped.push(json!({"label":label,"description":option["description"].as_str().unwrap_or("")}));
            }
            let multiple = match q.get("multiSelect") {
                None => false,
                Some(v) => v
                    .as_bool()
                    .ok_or_else(|| anyhow::anyhow!("Invalid multiSelect"))?,
            };
            questions.push(Question {
                text: text.into(),
                header: q["header"].as_str().unwrap_or("Question").into(),
                options: mapped,
                multiple,
            });
        }
        let picker = questions.iter().any(|q| q.multiple);
        let mut input = input.clone();
        input["answers"] = json!({});
        Ok(Self {
            input,
            questions,
            picker,
            answered: 0,
        })
    }

    pub fn next_request(&self) -> Option<(&'static str, Value)> {
        let question = self.questions.get(self.answered)?;
        if self.picker {
            Some((
                "item/tool/requestOptionPicker",
                json!({"question":question.text,"options":question.options,"allowMultiple":question.multiple,"submitLabel":"Continue","skipLabel":"Skip"}),
            ))
        } else {
            Some((
                "item/tool/requestUserInput",
                json!({"questions":self.questions.iter().enumerate().map(|(i,q)| json!({"id":format!("q{i}"),"header":q.header,"question":q.text,"options":q.options,"isOther":true,"isSecret":false})).collect::<Vec<_>>(),"isBlocking":true,"autoResolutionMs":null}),
            ))
        }
    }

    pub fn respond(&mut self, result: &Value) -> Result<Option<Value>> {
        ensure!(
            self.answered < self.questions.len(),
            "Question request already completed"
        );
        if self.picker {
            let answer = picker_answer(result, &self.questions[self.answered])?;
            self.input["answers"][&self.questions[self.answered].text] = json!(answer);
            self.answered += 1;
        } else {
            // Validate the entire response before committing any answer.
            let mut answers = serde_json::Map::new();
            for (i, q) in self.questions.iter().enumerate() {
                let values = result["answers"][format!("q{i}")]["answers"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("Missing question answer"))?;
                ensure!(
                    values.len() == 1,
                    "Single-select question requires one answer"
                );
                let answer = values[0]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| anyhow::anyhow!("Empty question answer"))?;
                answers.insert(q.text.clone(), json!(answer));
            }
            self.input["answers"] = json!(answers);
            self.answered = self.questions.len();
        }
        Ok((self.answered == self.questions.len()).then(|| self.input.clone()))
    }
}

fn picker_answer(result: &Value, question: &Question) -> Result<String> {
    if result["action"] != "submit" {
        bail!("Question dismissed or skipped");
    }
    let selected = result["selectedOptions"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing selected options"))?;
    ensure!(
        question.multiple || selected.len() <= 1,
        "Single-select question requires at most one selected option"
    );
    let mut answers: Vec<String> = Vec::new();
    for value in selected {
        let label = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid selected option"))?;
        ensure!(
            question.options.iter().any(|o| o["label"] == label),
            "Unknown selected option"
        );
        ensure!(
            !answers.iter().any(|s| s == label),
            "Duplicate selected option"
        );
        answers.push(label.into());
    }
    match result.get("freeformAnswer") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) if text.trim().is_empty() => {}
        Some(Value::String(text)) => answers.push(text.clone()),
        Some(_) => bail!("Invalid freeform answer"),
    }
    ensure!(!answers.is_empty(), "No answer selected");
    // This is the native AskUserQuestion answer format, including multi-select.
    Ok(answers.join(", "))
}
