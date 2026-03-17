use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;

/// Merges small Parquet files into larger ones
pub struct MergeCompactor {
    parquet_dir: PathBuf,
    min_size_bytes: u64,
}

impl MergeCompactor {
    pub fn new(parquet_dir: PathBuf, min_size_bytes: u64) -> Self {
        Self {
            parquet_dir,
            min_size_bytes,
        }
    }

    /// Run merge compaction
    pub async fn run(&self) -> Result<()> {
        // Find small parquet files
        let small_files = self.find_small_files()?;

        if small_files.len() < 2 {
            // Need at least 2 files to merge
            return Ok(());
        }

        tracing::info!(count = small_files.len(), "Found small parquet files to merge");

        // Generate merged filename
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");
        let merged_path = self.parquet_dir.join(format!("merged_{}.parquet", timestamp));

        // Merge files using DuckDB
        self.merge_files(&small_files, &merged_path)?;

        tracing::info!(
            source_count = small_files.len(),
            path = %merged_path.display(),
            "Merged parquet files"
        );

        // Delete original files only after successful merge
        for file in &small_files {
            if let Err(e) = std::fs::remove_file(file) {
                tracing::warn!(path = %file.display(), error = %e, "Failed to delete merged source file");
            }
        }

        Ok(())
    }

    fn find_small_files(&self) -> Result<Vec<PathBuf>> {
        let mut small_files = Vec::new();

        let entries = std::fs::read_dir(&self.parquet_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "parquet") {
                // Skip already merged files in this run
                if let Ok(metadata) = entry.metadata() {
                    if metadata.len() < self.min_size_bytes {
                        small_files.push(path);
                    }
                }
            }
        }

        // Sort by name (which includes timestamp) to maintain order
        small_files.sort();

        Ok(small_files)
    }

    fn merge_files(&self, files: &[PathBuf], output: &PathBuf) -> Result<()> {
        let conn = Connection::open_in_memory()?;

        // Build UNION ALL query for all files
        let file_queries: Vec<String> = files
            .iter()
            .map(|f| format!("SELECT * FROM read_parquet('{}')", f.display()))
            .collect();

        let union_query = file_queries.join(" UNION ALL ");

        // Create merged parquet
        let export_query = format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            union_query,
            output.display()
        );

        conn.execute_batch(&export_query)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_merge_compactor_no_files() {
        let dir = tempdir().unwrap();
        let compactor = MergeCompactor::new(dir.path().to_path_buf(), 1024);

        // Should not fail with empty directory
        compactor.run().await.unwrap();
    }
}
