# Fastly Log Sync and Analytics

A comprehensive tool for syncing Fastly logs from S3, parsing them, and generating analytics reports.

## ⚠️ Security Notice

**This tool processes log files that may contain sensitive information including:**

- IP addresses
- User agents
- Request paths and query parameters
- Response data
- Other potentially sensitive application data

**Please ensure:**

- Log files are stored securely and access is restricted
- Parsed log files are not committed to version control
- Configuration files with real bucket names/paths are gitignored
- Access to the dashboard and analytics is restricted to authorized personnel only

## Features

- **Date-filtered S3 sync**: Download logs for specific date ranges
- **Incremental sync**: Only downloads new files that don't already exist locally
- **UTC timezone handling**: All date operations use UTC to match log file organization
- **Log parsing**: Converts syslog-style Fastly logs to structured JSON/CSV
- **Comprehensive analytics**: Traffic patterns, error analysis, performance metrics, user agent analysis, query parameter patterns, endpoint drill-down, daily summaries, and slowness investigation
- **Interactive dashboard**: Streamlit-based web dashboard for visualizing log analytics
- **Modular design**: Run sync, parse, or analyze operations independently or as a pipeline

## Prerequisites

- **AWS CLI**: Required for S3 sync operations
  - Install: [https://aws.amazon.com/cli/](https://aws.amazon.com/cli/)
  - Configure credentials: `aws configure` or set environment variables
- **Python 3**: Required for parsing and analytics
- **Python packages**: Install with `pip install -r requirements.txt`

## Installation

1. Clone or download this repository
2. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Create your configuration file:

   ```bash
   cp config/log_sources.yaml.example config/log_sources.yaml
   ```

   Then edit `config/log_sources.yaml` with your actual log source configurations (S3 buckets, paths, etc.). This file is gitignored and won't be committed.

4. Ensure AWS CLI is installed and configured with appropriate credentials

## Usage

### Full Pipeline (Sync → Parse → Analyze)

Run all operations in sequence:

**From date to today** (syncs logs from the specified date to today, inclusive):

```bash
python3 scripts/query_logs.py --date 2025-11-10
```

**Date range** (syncs logs from start date to end date, inclusive):

```bash
python3 scripts/query_logs.py --start-date 2025-11-10 --end-date 2025-11-12
```

**Note**: The `--date` parameter syncs logs from the specified date **to today** (in UTC, since logs are stored in UTC). To sync a specific date range, use `--start-date` and `--end-date`.

**Timezone Note**: All date operations use UTC to match the log file organization in S3. Log timestamps are preserved in UTC throughout parsing and analysis.

### Individual Operations

#### Sync Only

Download logs from S3 without parsing or analyzing:

```bash
python3 scripts/query_logs.py --operation sync --start-date 2025-11-10 --end-date 2025-11-12
```

Or use the sync script directly:

```bash
python3 scripts/sync_logs.py --start-date 2025-11-10 --end-date 2025-11-12
```

#### Parse Only

Parse existing log files in the `logs/` directory:

```bash
python3 scripts/query_logs.py --operation parse
```

Or use the parser directly:

```bash
python3 scripts/parse_logs.py --input-dir ./logs/example-source-1/raw --output ./logs/example-source-1/parsed/parsed_logs.json
```

#### Analyze Only

Generate analytics from already-parsed logs:

```bash
python3 scripts/query_logs.py --operation analyze --parsed-output ./logs/example-source-1/parsed/parsed_logs.json
```

Or use the analyzer directly:

```bash
python3 scripts/analyze_logs.py --input ./logs/example-source-1/parsed/parsed_logs.json --format console
```

#### Interactive Dashboard

Launch the Streamlit dashboard for interactive visualization:

```bash
streamlit run dashboard/dashboard.py
```

The dashboard will open in your browser (typically at `http://localhost:8501`) and provides:

- **Traffic Patterns**: Requests over time, popular endpoints, HTTP method distribution
- **Error Analysis**: Status code breakdown, error rates, error-prone endpoints
- **Performance Metrics**: Cache hit/miss rates, response size statistics
- **User Agent Analysis**: Top user agents and agent type distribution
- **Query Patterns**: Most common query parameters and value distributions
- **Slowness Investigation**: Cache miss patterns, large response endpoints, peak traffic times

You can specify a custom parsed log file path in the dashboard sidebar.

## Command-Line Options

### query_logs.py

- `--start-date DATE`: Start date in YYYY-MM-DD format (required for sync)
- `--end-date DATE`: End date in YYYY-MM-DD format (required for sync)
- `--date DATE`: Start date in YYYY-MM-DD format (syncs from this date to today)
- `--operation OP`: Operation to perform: `sync`, `parse`, `analyze`, or `all` (default: `all`)
- `--parsed-output FILE`: Output file for parsed logs (default: first enabled source's parsed output)
- `--analytics-output FILE`: Output file for analytics report (optional)

### sync_logs.sh

- `--start-date DATE`: Start date in YYYY-MM-DD format
- `--end-date DATE`: End date in YYYY-MM-DD format
- `--date DATE`: Start date in YYYY-MM-DD format (syncs from this date to today)
- `-h, --help`: Show help message

### parse_logs.py

- `--input-dir DIR`: Directory containing log files (default: `./logs/example-source-1/raw`)
- `--output FILE`: Output file path (default: `./logs/example-source-1/parsed/parsed_logs.json`)
- `--format FORMAT`: Output format: `json` or `csv` (default: `json`)
- `--pattern PATTERN`: File pattern to match (default: `*.log*`)

### analyze_logs.py

- `--input FILE`: Input file (parsed JSON or CSV) - **required**
- `--output FILE`: Output file path (optional)
- `--format FORMAT`: Output format: `json` or `console` (default: `console`)

## Log Format

The tool parses Fastly logs in syslog format. All timestamps are in UTC (indicated by the 'Z' suffix):

```plaintext
<priority>timestamp cache-server process[pid]: IP "-" "-" date "METHOD path" status size "-" "user-agent" cache-status
```

Example:

```plaintext
<134>2025-11-09T23:57:35Z cache-server-001 s3logsprod[254840]: 192.0.2.1 "-" "-" Sun, 09 Nov 2025 23:57:35 GMT "GET /api/endpoint?param=value" 200 18508 "-" "Mozilla/5.0..." hit
```

**Note**: The example above uses dummy IP addresses (192.0.2.1 is a reserved test IP) and generic paths. Real log files will contain actual client IPs and request paths.

## Output Structure

### Parsed Logs (JSON)

Each log entry is parsed into structured fields:

```json
{
  "priority": 134,
  "timestamp": "2025-11-09T23:57:35",
  "cache_server": "cache-server-001",
  "ip_address": "192.0.2.1",
  "http_method": "GET",
  "path": "/api/endpoint",
  "query_string": "param=value",
  "query_params": { "param": "value" },
  "status_code": 200,
  "response_size": 18508,
  "user_agent": "Mozilla/5.0...",
  "cache_status": "hit"
}
```

**Note**: The example above uses dummy data. Real parsed logs will contain actual IP addresses, paths, and query parameters from your log files.

### Analytics Report

The analytics report includes:

- **Traffic Patterns**: Total requests, requests per hour/day/minute, popular endpoints, HTTP method distribution, peak traffic detection
- **Error Analysis**: Status code distribution, 4xx/5xx error rates, error-prone endpoints, hourly error breakdowns
- **Performance Metrics**: Cache hit/miss rates, response size statistics, hourly cache performance, hourly response size trends
- **User Agent Analysis**: Top user agents, agent type distribution
- **Query Patterns**: Most common query parameters, parameter value distributions, top query signatures
- **Slowness Investigation**: Traffic spikes, cache miss patterns, large response endpoints, peak traffic times, rate of change analysis
- **Endpoint Drill-Down**: Detailed analysis for specific endpoints (time patterns, errors, cache, query params)
- **Daily Summaries**: Daily request totals with status code breakdown by day

## File Structure

```plaintext
fastly_log_query/
├── config/
│   ├── log_sources.yaml.example  # Example configuration template
│   └── log_sources.yaml           # Log source configurations (gitignored, copy from .example)
├── logs/                    # Local log storage (created automatically, gitignored)
│   └── example-source-1/
│       ├── raw/             # Raw log files from S3 (gitignored)
│       └── parsed/          # Parsed log files (JSON/CSV, gitignored)
├── dashboard/
│   └── dashboard.py        # Streamlit interactive dashboard
├── scripts/
│   ├── query_logs.py        # Main orchestration script
│   ├── sync_logs.py         # S3 sync script
│   ├── parse_logs.py        # Log parser
│   └── analyze_logs.py      # Analytics engine
├── src/                     # Source code modules
│   ├── sync/                # Sync implementations
│   ├── parse/               # Parsing logic
│   ├── analyze/             # Analysis functions
│   └── utils/               # Utility functions
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Troubleshooting

### AWS Credentials Not Configured

If you see an error about AWS credentials:

```bash
aws configure
```

Or set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-southeast-2
```

### Python Dependencies Missing

Install required packages:

```bash
pip install -r requirements.txt
```

### No Log Files Found

Ensure:

1. The sync operation completed successfully
2. Log files are in the `logs/` directory
3. Files match the expected naming pattern (e.g., `2025-11-10T00:00:00.000--*.log.gz`)

### Security and Privacy

**Important considerations when working with log data:**

1. **Log files contain sensitive data**: IP addresses, user agents, request paths, and potentially sensitive query parameters
2. **Storage**: Ensure log files are stored securely with appropriate access controls
3. **Version control**: Never commit log files, parsed logs, or configuration files with real credentials/bucket names to git
4. **Access control**: Restrict access to the dashboard and analytics output to authorized personnel only
5. **Data retention**: Consider implementing data retention policies for log files
6. **Compliance**: Ensure log processing complies with your organization's data privacy policies and regulations

The `.gitignore` file is configured to exclude:

- `config/log_sources.yaml` (your actual configuration)
- `logs/` directory (all log files)
- Parsed log outputs

## Examples

### Example 1: Analyze logs from a date to today

```bash
python3 scripts/query_logs.py --date 2025-11-10  # Syncs from 2025-11-10 to today
```

### Example 2: Sync logs for a week, then analyze separately

```bash
# Sync
python3 scripts/query_logs.py --operation sync --start-date 2025-11-10 --end-date 2025-11-16

# Parse
python3 scripts/query_logs.py --operation parse

# Analyze with custom output
python3 scripts/query_logs.py --operation analyze --analytics-output ./reports/weekly_report.json
```

### Example 3: Parse and analyze existing logs

```bash
# Parse
python3 scripts/parse_logs.py --input-dir ./logs/example-source-1/raw --output ./logs/example-source-1/parsed/parsed.json --format json

# Analyze
python3 scripts/analyze_logs.py --input ./logs/example-source-1/parsed/parsed.json --format console --output ./reports/analysis.txt
```

### Example 4: Use the interactive dashboard

```bash
# First, parse your logs (if not already done)
python3 scripts/parse_logs.py --input-dir ./logs/example-source-1/raw --output ./logs/example-source-1/parsed/parsed_logs.json

# Then launch the dashboard
streamlit run dashboard/dashboard.py
```

The dashboard will automatically load the parsed logs and display interactive visualizations. You can change the log file path in the sidebar if needed.

## License

This tool is provided as-is for internal use.
