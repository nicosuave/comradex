//! Desktop function/namespace tools exposed through Claude's SDK MCP server.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{RpcError, RpcResult, required_str};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub namespace: Option<String>,
    pub mcp_name: String,
    pub description: String,
    pub input_schema: Value,
}

impl Tool {
    pub fn mcp_spec(&self) -> Value {
        json!({"name": self.mcp_name, "description": self.description,
            "inputSchema": self.input_schema})
    }

    pub fn validate_arguments(&self, arguments: &Value) -> RpcResult<()> {
        let validator = jsonschema::validator_for(&self.input_schema)
            .map_err(|_| RpcError::invalid("Invalid dynamic tool input schema"))?;
        if !validator.is_valid(arguments) {
            return Err(RpcError::invalid(
                "Dynamic tool arguments do not match its input schema",
            ));
        }
        Ok(())
    }
}

pub fn parse(value: &Value) -> RpcResult<Vec<Tool>> {
    let specs = value
        .as_array()
        .ok_or_else(|| RpcError::invalid("dynamicTools must be an array"))?;
    let mut tools = Vec::new();
    for spec in specs {
        if spec["type"] == "namespace" {
            let namespace = required_str(spec, "name")?;
            for tool in spec["tools"]
                .as_array()
                .ok_or_else(|| RpcError::invalid("Namespace tools must be an array"))?
            {
                add(&mut tools, tool, Some(namespace))?;
            }
        } else {
            add(&mut tools, spec, None)?;
        }
    }
    Ok(tools)
}

fn add(tools: &mut Vec<Tool>, spec: &Value, namespace: Option<&str>) -> RpcResult<()> {
    let name = required_str(spec, "name")?;
    if tools
        .iter()
        .any(|t| t.name == name && t.namespace.as_deref() == namespace)
    {
        return Err(RpcError::invalid(
            "Duplicate dynamic tool name in namespace",
        ));
    }
    if spec["type"] != "function" || !spec["inputSchema"].is_object() {
        return Err(RpcError::invalid(
            "Dynamic tools require a function with an object input schema",
        ));
    }
    jsonschema::validator_for(&spec["inputSchema"])
        .map_err(|_| RpcError::invalid("Invalid dynamic tool input schema"))?;
    let preferred = namespace.map_or_else(|| name.to_owned(), |ns| format!("{ns}__{name}"));
    // MCP names have a smaller alphabet than desktop namespace identifiers.
    // Preserve readable names when possible and retain exact routing separately.
    let mcp_name = if preferred.len() <= 64
        && preferred
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        && !tools.iter().any(|t| t.mcp_name == preferred)
        && !preferred.starts_with("desktop_tool_")
    {
        preferred
    } else {
        format!("desktop_tool_{}", tools.len())
    };
    tools.push(Tool {
        name: name.into(),
        namespace: namespace.map(str::to_owned),
        mcp_name,
        description: spec["description"].as_str().unwrap_or("").to_owned(),
        input_schema: spec["inputSchema"].clone(),
    });
    Ok(())
}

pub fn result_content(result: &Value) -> RpcResult<Value> {
    if !result["success"].is_boolean() {
        return Err(RpcError::invalid("Dynamic tool response needs success"));
    }
    let items = result["contentItems"]
        .as_array()
        .ok_or_else(|| RpcError::invalid("Dynamic tool response needs contentItems"))?;
    let mut content = Vec::new();
    for item in items {
        match item["type"].as_str() {
            Some("inputText") => content.push(
                json!({"type":"text", "text":required_str(item, "text").or_else(|e| {
                if item["text"] == "" { Ok("") } else { Err(e) }
            })?}),
            ),
            Some(kind @ ("inputImage" | "inputAudio")) => {
                let url = required_str(
                    item,
                    if kind == "inputImage" {
                        "imageUrl"
                    } else {
                        "audioUrl"
                    },
                )?;
                if let Some((mime, data)) = url
                    .strip_prefix("data:")
                    .and_then(|v| v.split_once(";base64,"))
                {
                    content.push(
                        json!({"type":if kind == "inputImage" {"image"} else {"audio"},
                        "mimeType":mime,"data":data}),
                    );
                } else {
                    content.push(json!({"type":"resource_link", "uri":url,
                        "name":if kind == "inputImage" {"Tool image"} else {"Tool audio"}}));
                }
            }
            _ => {
                return Err(RpcError::invalid(
                    "Unsupported dynamic tool response content",
                ));
            }
        }
    }
    Ok(json!({"content":content,"isError":result["success"] != true}))
}

pub fn failure(message: &str) -> Value {
    json!({"success":false,"contentItems":[{"type":"inputText","text":message}]})
}
