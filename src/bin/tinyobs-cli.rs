//! TinyObs CLI — standalone binary

use anyhow::Result;
use tinyobs::cli::{self, OutputFormat};
use tinyobs::client::TinyObsClient;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = cli::build_cli().get_matches();

    let endpoint = matches
        .get_one::<String>("endpoint")
        .map(|s| s.as_str())
        .unwrap_or(cli::DEFAULT_ENDPOINT);
    let format = matches
        .get_one::<String>("format")
        .map(|s| OutputFormat::from_str(s))
        .unwrap_or(OutputFormat::Table);

    let client = TinyObsClient::new(endpoint);

    let handled = cli::handle_command(&client, &matches, format).await?;
    if !handled {
        cli::build_cli().print_help()?;
    }

    Ok(())
}
