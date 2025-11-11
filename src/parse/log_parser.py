#!/usr/bin/env python3
"""
Fastly Log Parser
Parses syslog-style Fastly log entries and converts them to structured data.
"""

import re
import gzip
import json
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import sys


# Fastly log format regex pattern
# Format: <priority>timestamp cache-server process[pid]: IP "-" "-" date "METHOD path" status size "-" "user-agent" cache-status
LOG_PATTERN = re.compile(
    r'<(\d+)>'  # Priority code
    r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)'  # Timestamp
    r'\s+(\S+)'  # Cache server
    r'\s+(\S+)\[(\d+)\]:'  # Process and PID
    r'\s+(\S+)'  # IP address
    r'\s+"([^"]*)"'  # First "-" field (usually referrer)
    r'\s+"([^"]*)"'  # Second "-" field
    r'\s+([^"]+?)(?=\s+")'  # Date string (non-greedy until next quote)
    r'\s+"([A-Z]+)\s+([^"]+)"'  # HTTP method and path
    r'\s+(\d+)'  # Status code
    r'\s+(\d+)'  # Response size
    r'\s+"([^"]*)"'  # Referrer
    r'\s+"([^"]*)"'  # User agent
    r'\s+(\S+)'  # Cache status (hit/miss)
)


def safe_int(value, default=None):
    """Safely convert value to int, return default if fails."""
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default

def safe_get(groups, index, default=None):
    """Safely get group from regex match, return default if out of bounds."""
    try:
        return groups[index] if index < len(groups) and groups[index] else default
    except (IndexError, TypeError):
        return default

def parse_log_line(line: str) -> Optional[Dict]:
    """
    Parse a single log line and return structured data.
    Truly lazy parsing - extracts whatever fields are available using individual patterns.
    Doesn't rely on a fixed format - works with any log structure.

    Args:
        line: Raw log line string

    Returns:
        Dictionary with parsed fields or None if line is empty
    """
    line = line.strip()
    if not line:
        return None

    # Start with raw line
    result = {'raw_line': line}

    # Try LOG_PATTERN first as an optimization (faster for standard format)
    match = LOG_PATTERN.match(line)
    if match:
        groups = match.groups()
        result['priority'] = safe_int(safe_get(groups, 0))
        timestamp_str = safe_get(groups, 1)
        if timestamp_str:
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                result['timestamp'] = timestamp.isoformat()
            except (ValueError, TypeError):
                result['timestamp'] = None
        result['cache_server'] = safe_get(groups, 2)
        result['process'] = safe_get(groups, 3)
        result['pid'] = safe_int(safe_get(groups, 4))
        result['ip_address'] = safe_get(groups, 5)
        result['referrer1'] = safe_get(groups, 6)
        result['referrer2'] = safe_get(groups, 7)
        result['date_string'] = safe_get(groups, 8)
        method = safe_get(groups, 9)
        full_path = safe_get(groups, 10)
        if full_path:
            path_parts = full_path.split('?', 1)
            result['path'] = path_parts[0]
            result['query_string'] = path_parts[1] if len(path_parts) > 1 else None
            query_params = {}
            if result['query_string']:
                for param in result['query_string'].split('&'):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        query_params[key] = value
            result['query_params'] = query_params
        result['http_method'] = method
        result['status_code'] = safe_int(safe_get(groups, 11))
        result['response_size'] = safe_int(safe_get(groups, 12))
        result['referrer'] = safe_get(groups, 13)
        result['user_agent'] = safe_get(groups, 14)
        result['cache_status'] = safe_get(groups, 15)
        return result

    # Fallback: Extract fields individually (lazy mode - works with any format)
    # Extract timestamp (ISO format with Z)
    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
    if timestamp_match:
        try:
            timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%dT%H:%M:%SZ')
            result['timestamp'] = timestamp.isoformat()
        except ValueError:
            pass

    # Extract priority code
    priority_match = re.search(r'<(\d+)>', line)
    if priority_match:
        result['priority'] = safe_int(priority_match.group(1))

    # Extract IP address
    ip_match = re.search(r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b', line)
    if ip_match:
        result['ip_address'] = ip_match.group(1)

    # Extract HTTP method and path
    http_match = re.search(r'"([A-Z]+)\s+([^"]+)"', line)
    if http_match:
        result['http_method'] = http_match.group(1)
        full_path = http_match.group(2)
        path_parts = full_path.split('?', 1)
        result['path'] = path_parts[0]
        result['query_string'] = path_parts[1] if len(path_parts) > 1 else None
        # Parse query parameters
        if result.get('query_string'):
            query_params = {}
            for param in result['query_string'].split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    query_params[key] = value
            result['query_params'] = query_params
        else:
            result['query_params'] = {}

    # Extract status code (3-digit number)
    status_match = re.search(r'\s(\d{3})\s', line)
    if status_match:
        result['status_code'] = safe_int(status_match.group(1))

    # Extract response size (number after status code)
    size_match = re.search(r'\s(\d{3})\s+(\d+)\s', line)
    if size_match:
        result['response_size'] = safe_int(size_match.group(2))

    # Extract user agent (in quotes)
    ua_match = re.search(r'"([^"]*Mozilla[^"]*)"', line)
    if ua_match:
        result['user_agent'] = ua_match.group(1)
    else:
        # Try any quoted string that looks like a user agent
        ua_match = re.search(r'"([^"]{20,})"', line)
        if ua_match and 'Mozilla' in ua_match.group(1):
            result['user_agent'] = ua_match.group(1)

    # Extract cache status (hit/miss/etc)
    cache_match = re.search(r'\s(hit|miss|pass|error|synth)\s*$', line)
    if cache_match:
        result['cache_status'] = cache_match.group(1)

    # Extract cache server (word before process)
    server_match = re.search(r'cache-([^\s]+)', line)
    if server_match:
        result['cache_server'] = 'cache-' + server_match.group(1)

    # Extract process[pid]
    process_match = re.search(r'(\S+)\[(\d+)\]:', line)
    if process_match:
        result['process'] = process_match.group(1)
        result['pid'] = safe_int(process_match.group(2))

    return result


def process_log_file(file_path: Path):
    """
    Process a log file (compressed or uncompressed) and yield parsed entries lazily.

    Args:
        file_path: Path to the log file

    Yields:
        Parsed log entry dictionaries
    """
    try:
        if file_path.suffix == '.gz':
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    entry = parse_log_line(line)
                    if entry:
                        entry['source_file'] = str(file_path)
                        entry['line_number'] = line_num
                        yield entry
        else:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    entry = parse_log_line(line)
                    if entry:
                        entry['source_file'] = str(file_path)
                        entry['line_number'] = line_num
                        yield entry
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)


def save_json_streaming(entries, output_path: Path):
    """Save parsed data as JSON using streaming (memory efficient)."""
    with open(output_path, 'w') as f:
        f.write('[\n')
        first = True
        for entry in entries:
            if not first:
                f.write(',\n')
            json.dump(entry, f, indent=2)
            first = False
        f.write('\n]')


def save_csv_streaming(entries, output_path: Path):
    """Save parsed data as CSV using streaming (memory efficient)."""
    writer = None
    first_entry = True

    with open(output_path, 'w', newline='') as f:
        for entry in entries:
            # Flatten query_params for CSV
            flat_entry = entry.copy()
            # Convert query_params dict to string for CSV
            flat_entry['query_params'] = json.dumps(entry['query_params'])

            if first_entry:
                # Initialize writer with fieldnames from first entry
                fieldnames = flat_entry.keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                first_entry = False

            writer.writerow(flat_entry)


def main():
    parser = argparse.ArgumentParser(description='Parse Fastly log files')
    parser.add_argument(
        '--input-dir',
        type=str,
        default='./logs',
        help='Directory containing log files (default: ./logs)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (default: parsed_logs.json)'
    )
    parser.add_argument(
        '--format',
        choices=['json', 'csv'],
        default='json',
        help='Output format (default: json)'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        default='*.log*',
        help='File pattern to match (default: *.log*)'
    )

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # Find all log files
    log_files = []
    for pattern in [args.pattern, '*.log.gz', '*.log']:
        log_files.extend(input_dir.glob(pattern))

    # Remove duplicates
    log_files = list(set(log_files))

    if not log_files:
        print(f"No log files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(log_files)} log file(s)")
    print("Parsing logs (lazy/streaming mode - memory efficient)...")

    # Generator function that yields all entries from all files
    total_count = 0
    def all_entries_generator():
        nonlocal total_count
        for log_file in sorted(log_files):
            print(f"  Processing: {log_file.name}")
            file_count = 0
            for entry in process_log_file(log_file):
                file_count += 1
                total_count += 1
                yield entry
            print(f"    Parsed {file_count} entries")

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(args.input_dir) / f"parsed_logs.{args.format}"

    # Check if output file exists and warn
    if output_path.exists():
        print(f"Warning: Output file already exists: {output_path}")
        print(f"  It will be OVERWRITTEN with new data.")
        print(f"  (Previous data will be lost)")

    # Save output using streaming (overwrites existing file)
    print(f"Saving to {output_path}...")
    if args.format == 'json':
        save_json_streaming(all_entries_generator(), output_path)
    else:
        save_csv_streaming(all_entries_generator(), output_path)

    print(f"\nTotal entries parsed: {total_count}")

    print("Done!")


if __name__ == '__main__':
    main()

