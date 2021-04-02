use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub dir: PathBuf,
    pub value_dir: PathBuf,
    pub in_memory: bool,
    pub sync_write: bool,

    pub mem_table_siz: u64,
    pub base_table_siz: u64,
    pub base_level_siz: u64,
    pub level_siz_multiplier: usize,
    pub table_siz_multiplier: usize,
    pub max_levels: usize,

    pub value_threshold: usize,
    pub num_memtables: usize,

    pub block_siz: usize,
    pub bloom_false_positive: f64,

    pub num_level_zero_tables: usize,
    pub num_level_zero_tables_stall: usize,

    pub value_log_file_siz: u64,
    pub value_log_max_entries: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::new(),
            value_dir: PathBuf::new(),
            in_memory: false,
            sync_write: false,
            mem_table_siz: 64 << 20,
            base_table_siz: 2 << 20,
            base_level_siz: 10 << 20,
            level_siz_multiplier: 2,
            table_siz_multiplier: 10,
            max_levels: 7,
            value_threshold: 1 << 10,
            num_memtables: 20,
            block_siz: 4 << 10,
            bloom_false_positive: 0.5,
            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,
            value_log_file_siz: 1 << 30 - 1,
            value_log_max_entries: 1000000,
        }
    }
}