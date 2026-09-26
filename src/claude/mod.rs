//! Native Claude Code compatibility. No prompt, tool, or capability reconstruction.
pub mod auth;
pub(crate) mod maintenance;
pub(crate) mod quota;
pub(crate) mod wire;

pub const UPSTREAM: &str = "https://api.anthropic.com";
