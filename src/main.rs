use anyhow::{anyhow, Result};
use chrono::Utc;
use clap::Parser;
use csv::Reader;
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorAsyncClient};
use log::{error, info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// FalkorDB CSV Loader - Rust implementation
/// 
/// Loads nodes and edges from CSV files in the specified folder into FalkorDB.
/// Uses falkordb-rs client with batch processing and proper error handling.
#[derive(Parser)]
#[command(name = "falkordb-loader")]
#[command(about = "Load CSV files into FalkorDB")]
struct Args {
    /// Target graph name in FalkorDB
    graph_name: String,
    
    /// FalkorDB host
    #[arg(long, default_value = "localhost")]
    host: String,
    
    /// FalkorDB port
    #[arg(long, default_value_t = 6379)]
    port: u16,
    
    /// FalkorDB username (optional)
    #[arg(long)]
    username: Option<String>,
    
    /// FalkorDB password (optional)
    #[arg(long)]
    password: Option<String>,
    
    /// Batch size for loading
    #[arg(long, default_value_t = 5000)]
    batch_size: usize,
    
    /// Show graph statistics after loading
    #[arg(long)]
    stats: bool,
    
    /// Directory containing CSV files
    #[arg(long, default_value = "csv_output")]
    csv_dir: String,
    
    /// Use MERGE instead of CREATE for upsert behavior
    #[arg(long)]
    merge_mode: bool,
    
    /// Report progress every N records (0 disables progress reporting)
    #[arg(long, default_value_t = 1000)]
    progress_interval: usize,
    
    /// Enable fail-fast mode (terminate on first critical error)
    #[arg(long)]
    fail_fast: bool,
}

#[derive(Debug, Deserialize)]
struct IndexRecord {
    #[serde(default)]
    labels: String,
    #[serde(default)]
    properties: String,
    #[serde(default)]
    uniqueness: String,
    #[serde(rename = "type", default)]
    index_type: String,
}

#[derive(Debug, Deserialize)]
struct ConstraintRecord {
    #[serde(default)]
    labels: String,
    #[serde(default)]
    properties: String,
    #[serde(rename = "type", default)]
    constraint_type: String,
    #[serde(default)]
    entity_type: String,
}

/// Main FalkorDB CSV Loader struct
pub struct FalkorDBCSVLoader {
    client: FalkorAsyncClient,
    graph_name: String,
    csv_dir: PathBuf,
    merge_mode: bool,
    progress_interval: usize,
    /// Flag to indicate if loading should terminate on errors
    terminate_on_error: Arc<AtomicBool>,
    /// Maximum number of consecutive failures before terminating
    max_consecutive_failures: usize,
    /// Label mapping from edge labels to actual node labels
    label_mapping: HashMap<String, String>,
}

impl FalkorDBCSVLoader {
    /// Create a new FalkorDB CSV Loader instance
    pub async fn new(
        host: &str,
        port: u16,
        graph_name: String,
        csv_dir: String,
        username: Option<String>,
        password: Option<String>,
        merge_mode: bool,
        progress_interval: usize,
    ) -> Result<Self> {
        info!("Connecting to FalkorDB at {}:{}...", host, port);
        
        let falkor_url = match (username, password) {
            (Some(user), Some(pass)) => format!("falkor://{}:{}@{}:{}", user, pass, host, port),
            (Some(user), None) => format!("falkor://{}@{}:{}", user, host, port),
            _ => format!("falkor://{}:{}", host, port),
        };
        
        let connection_info: FalkorConnectionInfo = falkor_url.try_into()
            .map_err(|e| anyhow!("Invalid connection info: {:?}", e))?;
        
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(connection_info)
            .build()
            .await
            .map_err(|e| anyhow!("Failed to build client: {:?}", e))?;
        
        info!("✅ Connected to FalkorDB graph '{}'", graph_name);
        
        let loader = Self {
            client,
            graph_name,
            csv_dir: PathBuf::from(csv_dir),
            merge_mode,
            progress_interval,
            terminate_on_error: Arc::new(AtomicBool::new(false)),
            max_consecutive_failures: 3,
            label_mapping: HashMap::new(),
        };
        
        Ok(loader)
    }
    
    /// Execute a FalkorDB graph query with health checks
    async fn execute_graph_query(&self, query: &str) -> Result<()> {
        // Check if we should terminate
        if self.terminate_on_error.load(Ordering::Relaxed) {
            return Err(anyhow!("Loading terminated due to previous errors"));
        }
        
        let mut graph = self.client.select_graph(&self.graph_name);
        
        let _result = graph.query(query)
            .execute()
            .await
            .map_err(|e| {
                let error_msg = format!("{:?}", e).to_lowercase();
                if error_msg.contains("connection") || error_msg.contains("broken pipe") 
                   || error_msg.contains("reset") {
                    error!("❌ Connection error detected - FalkorDB may have crashed: {:?}", e);
                    self.terminate_on_error.store(true, Ordering::Relaxed);
                }
                anyhow!("Query execution failed: {:?}", e)
            })?;
        Ok(())
    }
    
    /// Execute a FalkorDB constraint command with error handling
    /// Note: For now, we'll use a simple query-based approach for constraint creation
    /// as the falkordb-rs library may handle constraints through graph queries
    async fn execute_constraint(&self, label: &str, properties: &[&str], constraint_type: &str, entity_type: &str) -> Result<()> {
        // Check if we should terminate
        if self.terminate_on_error.load(Ordering::Relaxed) {
            return Err(anyhow!("Loading terminated due to previous errors"));
        }
        
        let mut graph = self.client.select_graph(&self.graph_name);
        
        // Build constraint query - this might need adjustment based on FalkorDB's constraint syntax
        let query = if constraint_type.to_uppercase().contains("UNIQUE") && entity_type.to_uppercase() == "NODE" {
            if properties.len() == 1 {
                format!("CREATE CONSTRAINT FOR (n:{}) REQUIRE n.{} IS UNIQUE", label, properties[0])
            } else {
                let prop_list = properties.iter().map(|p| format!("n.{}", p)).collect::<Vec<_>>().join(", ");
                format!("CREATE CONSTRAINT FOR (n:{}) REQUIRE ({}) IS UNIQUE", label, prop_list)
            }
        } else {
            return Err(anyhow!("Unsupported constraint type: {} for entity type: {}", constraint_type, entity_type));
        };
        
        let _result = graph.query(&query)
            .execute()
            .await
            .map_err(|e| {
                let error_msg = format!("{:?}", e).to_lowercase();
                if error_msg.contains("connection") || error_msg.contains("broken pipe") 
                   || error_msg.contains("reset") {
                    error!("❌ Connection error in constraint creation: {:?}", e);
                    self.terminate_on_error.store(true, Ordering::Relaxed);
                }
                anyhow!("Constraint creation failed: {:?}", e)
            })?;
        Ok(())
    }
    
    /// Read a CSV file and return records as HashMap<String, String>
    fn read_csv_file<P: AsRef<Path>>(&self, file_path: P) -> Result<Vec<HashMap<String, String>>> {
        let file = File::open(&file_path)?;
        let mut rdr = Reader::from_reader(file);
        let mut records = Vec::new();
        
        for result in rdr.deserialize::<HashMap<String, String>>() {
            let record = result?;
            records.push(record);
        }
        
        info!("  Read {} rows from {:?}", records.len(), file_path.as_ref());
        Ok(records)
    }
    
    /// Sanitize label by replacing invalid characters
    fn sanitize_label(label: &str) -> String {
        label.replace(':', "_")
    }
    
    /// Validate and analyze label consistency between node and edge files
    pub fn validate_label_consistency(&self) -> Result<HashMap<String, String>> {
        info!("🔍 Validating label consistency between node and edge files...");
        
        // Get node labels from filenames
        let mut node_labels = std::collections::HashSet::new();
        let csv_files = std::fs::read_dir(&self.csv_dir)?;
        
        for entry in csv_files {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            
            if file_name.starts_with("nodes_") && file_name.ends_with(".csv") {
                let raw_label = file_name
                    .strip_prefix("nodes_")
                    .unwrap()
                    .strip_suffix(".csv")
                    .unwrap();
                let label = Self::sanitize_label(raw_label);
                node_labels.insert(label);
            }
        }
        
        info!("📋 Found node labels: {:?}", node_labels.iter().collect::<Vec<_>>());
        
        // Get edge labels from edge files
        let mut edge_labels = std::collections::HashSet::new();
        let csv_files = std::fs::read_dir(&self.csv_dir)?;
        
        for entry in csv_files {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            
            if file_name.starts_with("edges_") && file_name.ends_with(".csv") {
                let file_path = entry.path();
                
                // Read first data row to get labels
                let file = File::open(&file_path)?;
                let mut rdr = csv::Reader::from_reader(file);
                
                if let Some(result) = rdr.deserialize::<HashMap<String, String>>().next() {
                    let record = result?;
                    if let (Some(source_label), Some(target_label)) = 
                        (record.get("source_label"), record.get("target_label")) {
                        edge_labels.insert(source_label.clone());
                        edge_labels.insert(target_label.clone());
                    }
                }
            }
        }
        
        info!("📋 Found edge labels: {:?}", edge_labels.iter().collect::<Vec<_>>());
        
        // Create label mapping (case-insensitive matching)
        // Note: Edge labels can be multi-label (e.g., "Network:Zone") which means the node has both labels
        let mut label_mapping = HashMap::new();
        let mut missing_labels = Vec::new();
        
        for edge_label in &edge_labels {
            let mut found = false;
            
            // Try exact match first
            if node_labels.contains(edge_label) {
                label_mapping.insert(edge_label.clone(), edge_label.clone());
                found = true;
            } else {
                // Try case-insensitive match
                for node_label in &node_labels {
                    if node_label.to_lowercase() == edge_label.to_lowercase() {
                        label_mapping.insert(edge_label.clone(), node_label.clone());
                        info!("🔗 Mapped edge label '{}' -> node label '{}'", edge_label, node_label);
                        found = true;
                        break;
                    }
                }
                
                // If still not found, check if it's a multi-label (e.g., "Network:Zone")
                if !found && edge_label.contains(':') {
                    // Check if all parts of the multi-label exist as node labels
                    let label_parts: Vec<&str> = edge_label.split(':').collect();
                    let all_parts_exist = label_parts.iter().all(|part| {
                        // Check case-insensitive match for each part
                        node_labels.iter().any(|nl| nl.to_lowercase() == part.to_lowercase())
                    });
                    
                    if all_parts_exist {
                        // Multi-label is valid - keep it as-is (no mapping needed)
                        info!("✓ Multi-label '{}' is valid (all parts exist as node labels)", edge_label);
                        found = true;
                    }
                }
            }
            
            if !found {
                missing_labels.push(edge_label.clone());
            }
        }
        
        if !missing_labels.is_empty() {
            error!("❌ Found edge labels without corresponding node files: {:?}", missing_labels);
            return Err(anyhow!("Label validation failed: missing node files for labels: {:?}", missing_labels));
        }
        
        if label_mapping.is_empty() {
            info!("✅ All labels match exactly");
        } else {
            info!("✅ Label validation complete. Mappings: {:?}", label_mapping);
        }
        
        Ok(label_mapping)
    }
    
    /// Create ID indexes for all node labels
    pub async fn create_id_indexes_for_all_labels(&self) -> Result<()> {
        if !self.csv_dir.exists() {
            return Ok(());
        }
        
        info!("🔧 Creating ID indexes for all node labels...");
        
        let csv_files = std::fs::read_dir(&self.csv_dir)?;
        let mut created_count = 0;
        
        for entry in csv_files {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            
            if file_name.starts_with("nodes_") && file_name.ends_with(".csv") {
                // Extract label from filename
                let raw_label = file_name
                    .strip_prefix("nodes_")
                    .unwrap()
                    .strip_suffix(".csv")
                    .unwrap();
                let label = Self::sanitize_label(raw_label);
                
                let query = format!("CREATE INDEX ON :{}(id)", label);
                info!("  Creating ID index: {}", query);
                
                match self.execute_graph_query(&query).await {
                    Ok(_) => created_count += 1,
                    Err(e) => {
                        let error_msg = e.to_string().to_lowercase();
                        if error_msg.contains("already exists") || 
                           error_msg.contains("equivalent") || 
                           error_msg.contains("already indexed") || 
                           error_msg.contains("index exists") {
                            // Silently skip - index already exists
                        } else {
                            error!("  ❌ Error creating ID index on {}.id: {}", label, e);
                        }
                    }
                }
            }
        }
        
        if created_count > 0 {
            info!("✅ Created {} ID indexes", created_count);
        } else {
            info!("  No new ID indexes created");
        }
        
        Ok(())
    }
    
    /// Create indexes from indexes.csv file
    pub async fn create_indexes_from_csv(&self) -> Result<()> {
        let indexes_file = self.csv_dir.join("indexes.csv");
        if !indexes_file.exists() {
            warn!("⚠️ No indexes.csv file found, skipping index creation");
            return Ok(());
        }
        
        info!("🔧 Creating indexes from CSV...");
        let records = self.read_csv_file(&indexes_file)?;
        
        let mut created_count = 0;
        let mut skipped_count = 0;
        
        for record in records {
            let empty_string = String::new();
            let labels = record.get("labels").unwrap_or(&empty_string).trim();
            let properties = record.get("properties").unwrap_or(&empty_string).trim();
            let uniqueness = record.get("uniqueness").unwrap_or(&empty_string);
            let index_type = record.get("type").unwrap_or(&empty_string).to_uppercase();
            
            // Skip system indexes, unique constraints, and indexes without labels/properties
            if labels.is_empty() || properties.is_empty() || 
               index_type == "LOOKUP" || uniqueness == "UNIQUE" {
                skipped_count += 1;
                continue;
            }
            
            // Split labels and properties
            let label_list: Vec<&str> = labels.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            let prop_list: Vec<&str> = properties.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            // Create index for each label-property combination
            for label in &label_list {
                for prop in &prop_list {
                    let query = format!("CREATE INDEX ON :{}({})", label, prop);
                    info!("  Creating: {}", query);
                    
                    match self.execute_graph_query(&query).await {
                        Ok(_) => created_count += 1,
                        Err(e) => {
                            let error_msg = e.to_string().to_lowercase();
                            if error_msg.contains("already exists") || 
                               error_msg.contains("equivalent") || 
                               error_msg.contains("already indexed") || 
                               error_msg.contains("index exists") {
                                // Silently skip - index already exists
                            } else {
                                error!("  ❌ Error creating index on {}.{}: {}", label, prop, e);
                            }
                        }
                    }
                }
            }
        }
        
        info!("✅ Created {} indexes from CSV, skipped {}", created_count, skipped_count);
        Ok(())
    }
    
    /// Create supporting indexes for constraints
    pub async fn create_supporting_indexes_for_constraints(&self) -> Result<()> {
        let constraints_file = self.csv_dir.join("constraints.csv");
        if !constraints_file.exists() {
            return Ok(());
        }
        
        info!("🔧 Creating supporting indexes for constraints...");
        let records = self.read_csv_file(&constraints_file)?;
        
        if records.is_empty() {
            return Ok(());
        }
        
        let mut created_count = 0;
        
        for record in records {
            let empty_string = String::new();
            let labels = record.get("labels").unwrap_or(&empty_string).trim();
            let properties = record.get("properties").unwrap_or(&empty_string).trim();
            let constraint_type = record.get("type").unwrap_or(&empty_string).to_uppercase();
            
            // Only create indexes for UNIQUE constraints
            if labels.is_empty() || properties.is_empty() || !constraint_type.contains("UNIQUE") {
                continue;
            }
            
            // Split labels and properties
            let label_list: Vec<&str> = labels.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            let prop_list: Vec<&str> = properties.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            // Create supporting index for each label
            for label in &label_list {
                let query = if prop_list.len() == 1 {
                    format!("CREATE INDEX FOR (n:{}) ON (n.{})", label, prop_list[0])
                } else {
                    let prop_str: Vec<String> = prop_list.iter()
                        .map(|prop| format!("n.{}", prop))
                        .collect();
                    format!("CREATE INDEX FOR (n:{}) ON ({})", label, prop_str.join(", "))
                };
                
                info!("  Creating supporting index: {}", query);
                
                match self.execute_graph_query(&query).await {
                    Ok(_) => created_count += 1,
                    Err(e) => {
                        let error_msg = e.to_string().to_lowercase();
                        if error_msg.contains("already indexed") || 
                           error_msg.contains("already exists") || 
                           error_msg.contains("equivalent") || 
                           error_msg.contains("index exists") {
                            // Silently skip - supporting index already exists
                        } else {
                            error!("  ❌ Error creating supporting index for {}({}): {}", 
                                   label, prop_list.join(", "), e);
                        }
                    }
                }
            }
        }
        
        if created_count > 0 {
            info!("✅ Created {} supporting indexes", created_count);
        }
        
        Ok(())
    }
    
    /// Create constraints from constraints.csv file
    pub async fn create_constraints_from_csv(&self) -> Result<()> {
        let constraints_file = self.csv_dir.join("constraints.csv");
        if !constraints_file.exists() {
            warn!("⚠️ No constraints.csv file found, skipping constraint creation");
            return Ok(());
        }
        
        info!("🔒 Creating constraints...");
        let records = self.read_csv_file(&constraints_file)?;
        
        if records.is_empty() {
            info!("  No constraints to create");
            return Ok(());
        }
        
        let mut created_count = 0;
        let mut skipped_count = 0;
        
        for record in records {
            let empty_string = String::new();
            let labels = record.get("labels").unwrap_or(&empty_string).trim();
            let properties = record.get("properties").unwrap_or(&empty_string).trim();
            let constraint_type = record.get("type").unwrap_or(&empty_string).to_uppercase();
            let entity_type = record.get("entity_type").map_or("NODE", |v| v).to_uppercase();
            
            // Skip constraints without labels/properties
            if labels.is_empty() || properties.is_empty() {
                skipped_count += 1;
                continue;
            }
            
            // Split labels and properties
            let label_list: Vec<&str> = labels.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            let prop_list: Vec<&str> = properties.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            // Create constraint for each label
            for label in &label_list {
                if constraint_type.contains("UNIQUE") {
                    match self.execute_constraint(label, &prop_list, &constraint_type, &entity_type).await {
                        Ok(()) => {
                            created_count += 1;
                            info!("  ✅ Successfully created UNIQUE constraint on {}({})", 
                                  label, prop_list.join(", "));
                        }
                        Err(e) => {
                            let error_msg = e.to_string().to_lowercase();
                            if error_msg.contains("already exists") || 
                               error_msg.contains("constraint already exists") {
                                warn!("  ⚠️ Constraint on {}({}) already exists, skipping", 
                                      label, prop_list.join(", "));
                            } else {
                                error!("  ❌ Error creating constraint on {}({}): {}", 
                                       label, prop_list.join(", "), e);
                                skipped_count += 1;
                            }
                        }
                    }
                } else {
                    // Handle other constraint types if necessary
                    warn!("  ⚠️ Constraint type '{}' not supported by this loader, skipping {}.{:?}", 
                          constraint_type, label, prop_list);
                    skipped_count += 1;
                }
            }
        }
        
        if created_count > 0 {
            info!("✅ Created {} constraints", created_count);
        }
        if skipped_count > 0 {
            warn!("⚠️ Skipped {} constraints", skipped_count);
        }
        
        Ok(())
    }
    
    /// Parse value to appropriate type (treating all values as strings to avoid overflow)
    fn parse_value_for_property(value: &str) -> String {
        if value.is_empty() {
            return "None".to_string(); // Python uses None, not null
        }
        
        // Always treat values as quoted strings to avoid numeric overflow issues
        // This prevents i64/f64 parsing errors for very large numbers
        format!("'{}'", value.replace("'", "\\''"))
    }
    
    /// Parse ID value (separate from properties) - for node/edge IDs (always as strings to avoid overflow)
    fn parse_id_value(value: &str) -> String {
        if value.is_empty() {
            return "''".to_string();
        }
        
        // Always treat IDs as quoted strings to avoid any potential overflow issues
        // This ensures compatibility with very large numeric IDs that might overflow i64
        format!("'{}'", value.replace("'", "\\'"))
    }
    
    /// Parse value to appropriate JSON-compatible type for UNWIND parameters
    fn parse_value_to_json(value: &str) -> serde_json::Value {
        if value.is_empty() {
            return serde_json::Value::Null;
        }
        
        // Try to parse as number
        if let Ok(num) = value.parse::<i64>() {
            return serde_json::json!(num);
        }
        if let Ok(num) = value.parse::<f64>() {
            return serde_json::json!(num);
        }
        
        // Return as string
        serde_json::json!(value)
    }
    
    /// Convert serde_json::Value to Cypher literal syntax
    /// Cypher uses unquoted keys in maps: {key: value} not {"key": "value"}
    fn json_to_cypher_literal(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "null".to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => {
                // Escape single quotes and wrap in quotes
                format!("'{}'", s.replace("\\", "\\\\").replace("'", "\\'"))
            }
            serde_json::Value::Array(arr) => {
                let items: Vec<String> = arr.iter()
                    .map(|v| Self::json_to_cypher_literal(v))
                    .collect();
                format!("[{}]", items.join(", "))
            }
            serde_json::Value::Object(map) => {
                let items: Vec<String> = map.iter()
                    .map(|(k, v)| format!("{}: {}", k, Self::json_to_cypher_literal(v)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
    
    /// Load nodes from CSV file in batches using UNWIND for better performance
    pub async fn load_nodes_batch<P: AsRef<Path>>(&self, file_path: P, batch_size: usize) -> Result<()> {
        let start_time = Instant::now();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] Loading nodes from {:?}...", timestamp, file_path.as_ref());
        
        // Extract label from filename
        let filename = file_path.as_ref()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let raw_label = filename
            .strip_prefix("nodes_")
            .unwrap()
            .strip_suffix(".csv")
            .unwrap();
        let label = Self::sanitize_label(raw_label);
        
        let rows = self.read_csv_file(&file_path)?;
        if rows.is_empty() {
            return Ok(());
        }
        
        // Debug: show CSV headers
        if let Some(first_row) = rows.first() {
            let headers: Vec<&String> = first_row.keys().collect();
            info!("  CSV headers: {:?}", headers);
        }
        
        let mut total_loaded = 0;
        let total_records = rows.len();
        
        // Process in batches
        for (batch_num, batch) in rows.chunks(batch_size).enumerate() {
            let batch_start_time = Instant::now();
            
            // Report progress at the start of each batch if enabled
            if self.progress_interval > 0 && batch_num > 0 {
                let records_processed = batch_num * batch_size;
                if records_processed % (self.progress_interval * batch_size) == 0 || 
                   records_processed % self.progress_interval == 0 {
                    let progress = (records_processed as f64 / total_records as f64) * 100.0;
                    info!("📊 Progress: {:.1}% ({}/{}) {} nodes processed", 
                          progress, records_processed, total_records, label);
                }
            }
            
            // Check if we should terminate before processing batch
            if self.terminate_on_error.load(Ordering::Relaxed) {
                return Err(anyhow!("Loading terminated due to previous critical errors"));
            }
            
            // Build batch data for UNWIND - convert to JSON-compatible format
            let mut batch_data = Vec::new();
            
            for (j, row) in batch.iter().enumerate() {
                let empty_string = String::new();
                let node_id = row.get("id").unwrap_or(&empty_string);
                let mut properties = serde_json::Map::new();
                
                // Add all properties except id and labels
                for (key, value) in row {
                    if key != "id" && key != "labels" && !value.is_empty() {
                        properties.insert(key.clone(), Self::parse_value_to_json(value));
                    }
                }
                
                // Convert id to appropriate type
                let id_value = Self::parse_value_to_json(node_id);
                
                // Debug: show properties for first few records
                if batch_num == 0 && j < 3 {
                    info!("    Record {}: id = {:?}, properties = {:?}", j + 1, id_value, properties);
                }
                
                let mut node_data = serde_json::Map::new();
                node_data.insert("id".to_string(), id_value);
                node_data.insert("props".to_string(), serde_json::Value::Object(properties));
                
                batch_data.push(serde_json::Value::Object(node_data));
            }
            
            // Create single UNWIND query for the entire batch
            let unwind_query = if self.merge_mode {
                format!(
                    "UNWIND $batch AS row MERGE (n:{} {{id: row.id}}) SET n += row.props",
                    label
                )
            } else {
                format!(
                    "UNWIND $batch AS row CREATE (n:{}) SET n.id = row.id, n += row.props",
                    label
                )
            };
            
            // Debug: show generated query for first batch
            if batch_num == 0 {
                info!("    Generated UNWIND query: {}", unwind_query);
                info!("    Batch size: {} nodes", batch_data.len());
            }
            
            // Execute UNWIND query with batch data using JSON parameters from PR #138
            let mut graph = self.client.select_graph(&self.graph_name);
            let batch_json_value = serde_json::Value::Array(batch_data.clone());
            let mut params = HashMap::new();
            params.insert("batch".to_string(), batch_json_value);
            
            let result = graph.query(&unwind_query)
                .with_json_params(&params)
                .execute()
                .await;
            
            match result {
                Ok(_) => {
                    total_loaded += batch.len();
                    
                    // Report progress for batch
                    if self.progress_interval > 0 {
                        let progress = (total_loaded as f64 / total_records as f64) * 100.0;
                        if total_loaded % self.progress_interval <= batch.len() || 
                           total_loaded == total_records {
                            info!("📊 Progress: {:.1}% ({}/{}) {} nodes loaded", 
                                  progress, total_loaded, total_records, label);
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Error loading batch with UNWIND: {}", e);
                    error!("Falling back to individual queries for this batch...");
                    
                    // Fallback to individual queries if batch fails
                    let mut successful_nodes = 0;
                    for row in batch.iter() {
                        let empty_string = String::new();
                        let node_id = row.get("id").unwrap_or(&empty_string);
                        let mut properties = Vec::new();
                        
                        for (key, value) in row {
                            if key != "id" && key != "labels" && !value.is_empty() {
                                let parsed_value = Self::parse_value_for_property(value);
                                if parsed_value != "None" {
                                    properties.push(format!("{}: {}", key, parsed_value));
                                }
                            }
                        }
                        
                        let id_str = Self::parse_id_value(node_id);
                        
                        let node_query = if self.merge_mode {
                            if properties.is_empty() {
                                format!("MERGE (:{} {{id: {}}})", label, id_str)
                            } else {
                                format!("MERGE (:{} {{id: {}, {}}})", label, id_str, properties.join(", "))
                            }
                        } else {
                            if properties.is_empty() {
                                format!("CREATE (:{} {{id: {}}})", label, id_str)
                            } else {
                                format!("CREATE (:{} {{id: {}, {}}})", label, id_str, properties.join(", "))
                            }
                        };
                        
                        match self.execute_graph_query(&node_query).await {
                            Ok(_) => successful_nodes += 1,
                            Err(e2) => {
                                error!("❌ Error loading node: {}", e2);
                                error!("Query: {}", node_query);
                            }
                        }
                    }
                    
                    total_loaded += successful_nodes;
                    if successful_nodes != batch.len() {
                        warn!("⚠️ Loaded {} out of {} nodes in this batch", successful_nodes, batch.len());
                    }
                }
            }
            
            let batch_duration = batch_start_time.elapsed();
            let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
            info!("[{}] Batch complete: Loaded {} nodes (Duration: {:?})", 
                  timestamp, batch.len(), batch_duration);
        }
        
        let duration = start_time.elapsed();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] ✅ Loaded {} {} nodes (Duration: {:?})", 
              timestamp, total_loaded, label, duration);
        
        Ok(())
    }
    
    /// Load edges from CSV file in batches using UNWIND for better performance
    pub async fn load_edges_batch<P: AsRef<Path>>(&self, file_path: P, batch_size: usize) -> Result<()> {
        let start_time = Instant::now();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] Loading edges from {:?}...", timestamp, file_path.as_ref());
        
        // Extract relationship type from filename
        let filename = file_path.as_ref()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let rel_type = filename
            .strip_prefix("edges_")
            .unwrap()
            .strip_suffix(".csv")
            .unwrap();
        
        let rows = self.read_csv_file(&file_path)?;
        if rows.is_empty() {
            return Ok(());
        }
        
        let mut total_loaded = 0;
        let total_records = rows.len();
        
        // Process in batches
        for (batch_num, batch) in rows.chunks(batch_size).enumerate() {
            let batch_start_time = Instant::now();
            
            // Report progress at the start of each batch if enabled
            if self.progress_interval > 0 && batch_num > 0 {
                let records_processed = batch_num * batch_size;
                if records_processed % (self.progress_interval * batch_size) == 0 || 
                   records_processed % self.progress_interval == 0 {
                    let progress = (records_processed as f64 / total_records as f64) * 100.0;
                    info!("📊 Progress: {:.1}% ({}/{}) {} edges processed", 
                          progress, records_processed, total_records, rel_type);
                }
            }
            
            // Check if we should terminate before processing batch
            if self.terminate_on_error.load(Ordering::Relaxed) {
                return Err(anyhow!("Loading terminated due to previous critical errors"));
            }
            
            // Build batch data for UNWIND - convert to JSON-compatible format
            let mut batch_data = Vec::new();
            let mut first_source_label = String::new();
            let mut first_target_label = String::new();
            
            for (j, row) in batch.iter().enumerate() {
                let empty_string = String::new();
                let source_id = row.get("source").unwrap_or(&empty_string);
                let target_id = row.get("target").unwrap_or(&empty_string);
                
                if source_id.is_empty() || target_id.is_empty() {
                    continue;
                }
                
                let mut properties = serde_json::Map::new();
                
                // Get source and target labels if available
                let raw_source_label = row.get("source_label").unwrap_or(&empty_string).trim();
                let raw_target_label = row.get("target_label").unwrap_or(&empty_string).trim();
                
                // Apply label mapping to resolve case mismatches
                let source_label = self.label_mapping.get(raw_source_label)
                    .map_or(raw_source_label, |s| s.as_str());
                let target_label = self.label_mapping.get(raw_target_label)
                    .map_or(raw_target_label, |s| s.as_str());
                
                // Get first label for nodes (handle multiple labels)
                let source_label_first = source_label.split(':').next().unwrap_or(source_label);
                let target_label_first = target_label.split(':').next().unwrap_or(target_label);
                
                // Store first labels for query construction
                if j == 0 {
                    first_source_label = source_label_first.to_string();
                    first_target_label = target_label_first.to_string();
                }
                
                // Add all properties except source, target, type, source_label, target_label
                for (key, value) in row {
                    if !["source", "target", "type", "source_label", "target_label"].contains(&key.as_str()) 
                       && !value.is_empty() {
                        // Clean up property key: remove duplicate prefixes like 'Date:Date' -> 'Date'
                        let clean_key = if key.contains(':') {
                            let parts: Vec<&str> = key.split(':').collect();
                            if parts.len() == 2 && parts[0] == parts[1] {
                                parts[0].to_string()
                            } else {
                                key.clone()
                            }
                        } else {
                            key.clone()
                        };
                        
                        properties.insert(clean_key, Self::parse_value_to_json(value));
                    }
                }
                
                // Convert IDs to appropriate type
                let source_id_value = Self::parse_value_to_json(source_id);
                let target_id_value = Self::parse_value_to_json(target_id);
                
                // Debug: show label usage for first few records
                if batch_num == 0 && j < 3 {
                    info!("    Record {}: raw_source_label='{}' -> '{}', raw_target_label='{}' -> '{}'", 
                          j + 1, raw_source_label, source_label_first, raw_target_label, target_label_first);
                }
                
                let mut edge_data = serde_json::Map::new();
                edge_data.insert("source_id".to_string(), source_id_value);
                edge_data.insert("target_id".to_string(), target_id_value);
                edge_data.insert("source_label".to_string(), serde_json::json!(source_label_first));
                edge_data.insert("target_label".to_string(), serde_json::json!(target_label_first));
                edge_data.insert("props".to_string(), serde_json::Value::Object(properties));
                
                batch_data.push(serde_json::Value::Object(edge_data));
            }
            
            if batch_data.is_empty() {
                continue;
            }
            
            // Create single UNWIND query for the entire batch
            // NOTE: We match by ID only without label filtering because:
            // 1. Nodes may have multi-labels (e.g., "OS:Process") and using only first label fails
            // 2. ID is unique across the graph and indexed, so matching by ID is efficient
            let unwind_query = if self.merge_mode {
                format!(
                    "UNWIND $batch AS row \
                     MERGE (a {{id: row.source_id}}) \
                     MERGE (b {{id: row.target_id}}) \
                     MERGE (a)-[r:{}]->(b) \
                     SET r += row.props",
                    rel_type
                )
            } else {
                format!(
                    "UNWIND $batch AS row \
                     MATCH (a {{id: row.source_id}}) \
                     MATCH (b {{id: row.target_id}}) \
                     CREATE (a)-[r:{}]->(b) \
                     SET r += row.props",
                    rel_type
                )
            };
            
            // Debug: show generated query for first batch
            if batch_num == 0 {
                info!("    Generated UNWIND query: {}", unwind_query);
                info!("    Batch size: {} edges", batch_data.len());
                if self.merge_mode {
                    info!("    Using MERGE mode for relationships");
                } else {
                    info!("    Using CREATE mode for relationships");
                }
            }
            
            // Execute UNWIND query with batch data using JSON parameters from PR #138
            let mut graph = self.client.select_graph(&self.graph_name);
            let batch_json_value = serde_json::Value::Array(batch_data.clone());
            let mut params = HashMap::new();
            params.insert("batch".to_string(), batch_json_value);
            
            let result = graph.query(&unwind_query)
                .with_json_params(&params)
                .execute()
                .await;
            
            match result {
                Ok(_) => {
                    total_loaded += batch_data.len();
                    
                    // Report progress for batch
                    if self.progress_interval > 0 {
                        let progress = (total_loaded as f64 / total_records as f64) * 100.0;
                        if total_loaded % self.progress_interval <= batch_data.len() || 
                           total_loaded == total_records {
                            info!("📊 Progress: {:.1}% ({}/{}) {} edges loaded", 
                                  progress, total_loaded, total_records, rel_type);
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Error loading batch with UNWIND: {}", e);
                    error!("Falling back to individual queries for this batch...");
                    
                    // Fallback to individual queries if batch fails
                    let mut successful_edges = 0;
                    for row in batch.iter() {
                        let empty_string = String::new();
                        let source_id = row.get("source").unwrap_or(&empty_string);
                        let target_id = row.get("target").unwrap_or(&empty_string);
                        
                        if source_id.is_empty() || target_id.is_empty() {
                            continue;
                        }
                        
                        let mut properties = Vec::new();
                        let raw_source_label = row.get("source_label").unwrap_or(&empty_string).trim();
                        let raw_target_label = row.get("target_label").unwrap_or(&empty_string).trim();
                        
                        let source_label = self.label_mapping.get(raw_source_label)
                            .map_or(raw_source_label, |s| s.as_str());
                        let target_label = self.label_mapping.get(raw_target_label)
                            .map_or(raw_target_label, |s| s.as_str());
                        
                        for (key, value) in row {
                            if !["source", "target", "type", "source_label", "target_label"].contains(&key.as_str()) 
                               && !value.is_empty() {
                                let parsed_value = Self::parse_value_for_property(value);
                                if parsed_value != "None" {
                                    properties.push(format!("{}: {}", key, parsed_value));
                                }
                            }
                        }
                        
                        let source_id_str = Self::parse_id_value(source_id);
                        let target_id_str = Self::parse_id_value(target_id);
                        
                        // Match by ID only (without labels) to handle multi-label nodes correctly
                        let edge_query = if self.merge_mode {
                            let prop_set = if properties.is_empty() {
                                String::new()
                            } else {
                                format!(" SET {}", properties.iter()
                                        .map(|p| format!("r.{}", p))
                                        .collect::<Vec<_>>()
                                        .join(", "))
                            };
                            format!("MERGE (a {{id: {}}}) MERGE (b {{id: {}}}) MERGE (a)-[r:{}]->(b){}",
                                    source_id_str, target_id_str, rel_type, prop_set)
                        } else {
                            let prop_str = if properties.is_empty() {
                                String::new()
                            } else {
                                format!(" {{{}}}", properties.join(", "))
                            };
                            format!("MATCH (a {{id: {}}}), (b {{id: {}}}) CREATE (a)-[:{}{}]->(b)",
                                    source_id_str, target_id_str, rel_type, prop_str)
                        };
                        
                        match self.execute_graph_query(&edge_query).await {
                            Ok(_) => successful_edges += 1,
                            Err(e2) => {
                                error!("❌ Error loading edge: {}", e2);
                                error!("Query: {}", edge_query);
                            }
                        }
                    }
                    
                    total_loaded += successful_edges;
                    if successful_edges != batch.len() {
                        warn!("⚠️ Loaded {} out of {} edges in this batch", successful_edges, batch.len());
                    }
                }
            }
            
            let batch_duration = batch_start_time.elapsed();
            let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
            info!("[{}] Batch complete: Loaded {} edges (Duration: {:?})", 
                  timestamp, batch_data.len(), batch_duration);
        }
        
        let duration = start_time.elapsed();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] ✅ Loaded {} {} relationships (Duration: {:?})", 
              timestamp, total_loaded, rel_type, duration);
        
        Ok(())
    }
    
    /// Count total records across all CSV files for progress tracking
    fn count_total_records(&self, files: &[std::path::PathBuf]) -> Result<usize> {
        let mut total = 0;
        for file_path in files {
            if let Ok(file) = std::fs::File::open(file_path) {
                let mut rdr = csv::Reader::from_reader(file);
                total += rdr.records().count();
            }
        }
        Ok(total)
    }
    
    /// Check for potential crash causes and system resource issues
    async fn check_system_health(&self) -> Result<()> {
        info!("🔍 Checking system health before loading...");
        
        // Test basic connectivity
        match self.execute_graph_query("RETURN 1 as test").await {
            Ok(_) => info!("✓ FalkorDB connectivity: OK"),
            Err(e) => {
                error!("❌ FalkorDB connectivity test failed: {}", e);
                return Err(anyhow!("FalkorDB is not responsive: {}", e));
            }
        }
        
        // Test memory allocation with a small query
        let test_query = "CREATE (test:TestNode {id: 'health_check', timestamp: timestamp()}) RETURN test";
        match self.execute_graph_query(test_query).await {
            Ok(_) => {
                info!("✓ FalkorDB memory allocation: OK");
                // Clean up test node
                let _ = self.execute_graph_query("MATCH (test:TestNode {id: 'health_check'}) DELETE test").await;
            }
            Err(e) => {
                warn!("⚠️ FalkorDB may have memory issues: {}", e);
            }
        }
        
        // Warn about large batch sizes in merge mode
        if self.merge_mode {
            warn!("⚠️ Running in MERGE mode - this generates complex queries that may strain FalkorDB");
            warn!("   Consider using smaller batch sizes or CREATE mode for initial loads");
        }
        
        Ok(())
    }
    
    /// Load all CSV files from the csv_output directory
    pub async fn load_all_csvs(&mut self, batch_size: usize) -> Result<()> {
        if !self.csv_dir.exists() {
            return Err(anyhow!("Directory {:?} does not exist", self.csv_dir));
        }
        
        // Validate label consistency first
        let label_mapping = self.validate_label_consistency()?;
        self.label_mapping = label_mapping;
        
        let csv_files = std::fs::read_dir(&self.csv_dir)?;
        let mut node_files = Vec::new();
        let mut edge_files = Vec::new();
        
        for entry in csv_files {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            
            if file_name.starts_with("nodes_") && file_name.ends_with(".csv") {
                node_files.push(entry.path());
            } else if file_name.starts_with("edges_") && file_name.ends_with(".csv") {
                edge_files.push(entry.path());
            }
        }
        
        info!("Found {} node files and {} edge files", node_files.len(), edge_files.len());
        
        // Count total records for progress tracking if enabled
        let (total_node_records, total_edge_records) = if self.progress_interval > 0 {
            let node_count = self.count_total_records(&node_files).unwrap_or(0);
            let edge_count = self.count_total_records(&edge_files).unwrap_or(0);
            info!("📊 Total records to process: {} nodes, {} edges", node_count, edge_count);
            (node_count, edge_count)
        } else {
            (0, 0)
        };
        
        // Check system health first
        self.check_system_health().await?;
        
        // Create indexes and constraints first (for better performance)
        info!("\n🗼️ Setting up database schema...");
        self.create_id_indexes_for_all_labels().await?;
        self.create_indexes_from_csv().await?;
        self.create_supporting_indexes_for_constraints().await?;
        self.create_constraints_from_csv().await?;
        
        // Load nodes first
        let nodes_start_time = Instant::now();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("\n[{}] 📥 Loading nodes...", timestamp);
        
        let mut total_nodes_loaded = 0;
        for (file_idx, node_file) in node_files.iter().enumerate() {
            if self.progress_interval > 0 {
                info!("📁 Processing node file {}/{}: {:?}", 
                      file_idx + 1, node_files.len(), node_file.file_name().unwrap_or_default());
            }
            
            let file_records = if self.progress_interval > 0 {
                // Count records in this file for progress tracking
                std::fs::File::open(node_file)
                    .map(|f| csv::Reader::from_reader(f).records().count())
                    .unwrap_or(0)
            } else {
                0
            };
            
            // Check for termination before processing each file
            if self.terminate_on_error.load(Ordering::Relaxed) {
                return Err(anyhow!("Loading terminated due to critical errors in previous operations"));
            }
            
            match self.load_nodes_batch(node_file, batch_size).await {
                Ok(_) => {
                    info!("✓ Successfully loaded node file: {:?}", node_file.file_name().unwrap_or_default());
                }
                Err(e) => {
                    error!("❌ Failed to load node file {:?}: {}", node_file.file_name().unwrap_or_default(), e);
                    self.terminate_on_error.store(true, Ordering::Relaxed);
                    return Err(anyhow!("Critical error loading nodes from {:?}: {}", node_file, e));
                }
            }
            
            total_nodes_loaded += file_records;
            if self.progress_interval > 0 && total_node_records > 0 {
                let overall_progress = (total_nodes_loaded as f64 / total_node_records as f64) * 100.0;
                info!("🎯 Overall node progress: {:.1}% ({}/{})", 
                      overall_progress, total_nodes_loaded, total_node_records);
            }
        }
        
        let nodes_duration = nodes_start_time.elapsed();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] ✅ All nodes loaded (Total duration: {:?})", timestamp, nodes_duration);
        
        // Then load edges
        let edges_start_time = Instant::now();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("\n[{}] 🔗 Loading edges...", timestamp);
        
        let mut total_edges_loaded = 0;
        for (file_idx, edge_file) in edge_files.iter().enumerate() {
            if self.progress_interval > 0 {
                info!("📁 Processing edge file {}/{}: {:?}", 
                      file_idx + 1, edge_files.len(), edge_file.file_name().unwrap_or_default());
            }
            
            let file_records = if self.progress_interval > 0 {
                // Count records in this file for progress tracking
                std::fs::File::open(edge_file)
                    .map(|f| csv::Reader::from_reader(f).records().count())
                    .unwrap_or(0)
            } else {
                0
            };
            
            // Check for termination before processing each file
            if self.terminate_on_error.load(Ordering::Relaxed) {
                return Err(anyhow!("Loading terminated due to critical errors in previous operations"));
            }
            
            match self.load_edges_batch(edge_file, batch_size).await {
                Ok(_) => {
                    info!("✓ Successfully loaded edge file: {:?}", edge_file.file_name().unwrap_or_default());
                }
                Err(e) => {
                    error!("❌ Failed to load edge file {:?}: {}", edge_file.file_name().unwrap_or_default(), e);
                    self.terminate_on_error.store(true, Ordering::Relaxed);
                    return Err(anyhow!("Critical error loading edges from {:?}: {}", edge_file, e));
                }
            }
            
            total_edges_loaded += file_records;
            if self.progress_interval > 0 && total_edge_records > 0 {
                let overall_progress = (total_edges_loaded as f64 / total_edge_records as f64) * 100.0;
                info!("🎯 Overall edge progress: {:.1}% ({}/{})", 
                      overall_progress, total_edges_loaded, total_edge_records);
            }
        }
        
        let edges_duration = edges_start_time.elapsed();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("[{}] ✅ All edges loaded (Total duration: {:?})", timestamp, edges_duration);
        
        let total_duration = nodes_start_time.elapsed();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
        info!("\n[{}] ✅ Successfully loaded data into graph '{}' (Total loading time: {:?})",
              timestamp, self.graph_name, total_duration);
        
        Ok(())
    }
    
    /// Verify node attributes for a specific node type
    pub async fn verify_node_attributes(&self, label: &str, limit: usize) -> Result<()> {
        let query = format!("MATCH (n:{}) RETURN n LIMIT {}", label, limit);
        match self.execute_graph_query(&query).await {
            Ok(result) => {
                info!("\n🔍 Sample {} nodes with their attributes:", label);
                info!("Result: {:?}", result);
            }
            Err(e) => {
                error!("❌ Error verifying node attributes: {}", e);
            }
        }
        Ok(())
    }
    
    /// Get statistics about the loaded graph
    pub async fn get_graph_stats(&self) -> Result<()> {
        info!("\n📊 Graph Statistics:");
        
        // Count nodes by label
        info!("Nodes:");
        let node_query = "MATCH (n) RETURN labels(n) as labels, count(n) as count";
        match self.execute_graph_query(node_query).await {
            Ok(result) => {
                info!("Node stats result: {:?}", result);
            }
            Err(e) => {
                error!("❌ Error getting node statistics: {}", e);
            }
        }
        
        // Count relationships by type
        info!("Relationships:");
        let rel_query = "MATCH ()-[r]->() RETURN type(r) as type, count(r) as count";
        match self.execute_graph_query(rel_query).await {
            Ok(result) => {
                info!("Relationship stats result: {:?}", result);
            }
            Err(e) => {
                error!("❌ Error getting relationship statistics: {}", e);
            }
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args = Args::parse();
    
    let mut loader = FalkorDBCSVLoader::new(
        &args.host,
        args.port,
        args.graph_name,
        args.csv_dir,
        args.username,
        args.password,
        args.merge_mode,
        args.progress_interval,
    ).await?;
    
    // Load everything (indexes, constraints, and data)
    match loader.load_all_csvs(args.batch_size).await {
        Ok(_) => {
            if args.stats {
                loader.get_graph_stats().await?;
                loader.verify_node_attributes("Person", 3).await?;
            }
        }
        Err(e) => {
            error!("❌ Loading failed: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}