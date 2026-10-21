use crate::tool::{ToolCapabilityOverrides, ToolCategory, ToolDefinition, ToolSource};
pub fn read_file_definition() -> ToolDefinition {
    ToolDefinition {
        name: "read_file".into(),
        description: "Read the contents of a file.".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Path to the file" }
            },
            "required": ["path"]
        }),
        category: ToolCategory::Query,
        capability_overrides: ToolCapabilityOverrides::default(),
        source: ToolSource::Builtin,
    }
}
pub fn write_file_definition() -> ToolDefinition {
    ToolDefinition {
        name: "write_file".into(),
        description: "Write content to a file.".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Path to the file" },
                "content": { "type": "string", "description": "Content to write" }
            },
            "required": ["path", "content"]
        }),
        category: ToolCategory::Mutation,
        capability_overrides: ToolCapabilityOverrides::default(),
        source: ToolSource::Builtin,
    }
}
pub fn search_files_definition() -> ToolDefinition {
    ToolDefinition {
        name: "search_files".into(),
        description: "Search for files using a given pattern (rg regex).".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "cwd": { "type": "string", "description": "Directory to search in" },
                "pattern": { "type": "string", "description": "Regex pattern to search for" }
            },
            "required": ["cwd", "pattern"]
        }),
        category: ToolCategory::Query,
        capability_overrides: ToolCapabilityOverrides::default(),
        source: ToolSource::Builtin,
    }
}
pub fn list_directory_definition() -> ToolDefinition {
    ToolDefinition {
        name: "list_directory".into(),
        description: "List the contents of a directory, returning properties like size and type."
            .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Absolute path to the directory" }
            },
            "required": ["path"]
        }),
        category: ToolCategory::Query,
        capability_overrides: ToolCapabilityOverrides::default(),
        source: ToolSource::Builtin,
    }
}
