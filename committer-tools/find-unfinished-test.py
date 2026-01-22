# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
from datetime import datetime
from typing import Optional, Tuple, Dict


def pretty_time_duration(seconds: float) -> str:
    """Format duration in seconds as a human-readable string (e.g., '1h23m45s')."""
    time_min, time_sec = divmod(int(seconds), 60)
    time_hour, time_min = divmod(time_min, 60)
    time_fmt = ""
    if time_hour > 0:
        time_fmt += f"{time_hour}h"
    if time_min > 0:
        time_fmt += f"{time_min}m"
    time_fmt += f"{time_sec}s"
    return time_fmt


def parse_test_line(line: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse a Gradle test log line.
    
    Returns:
        Tuple of (test_name, status, timestamp) if line is valid, None otherwise.
    """
    try:
        if "Gradle Test Run" not in line:
            return None
        
        # Extract timestamp (first token before space)
        parts = line.strip().split(" ", 1)
        if len(parts) < 2:
            return None
        timestamp = parts[0]
        
        # Parse test name and status
        toks = parts[1].split(" > ")
        if len(toks) < 3:
            return None
        
        # Last token should be "name STATUS"
        name_status = toks[-1].rsplit(" ", 1)
        if len(name_status) != 2:
            return None
        
        name, status = name_status
        name_toks = toks[2:-1] + [name]
        test = " > ".join(name_toks)
        
        return (test, status, timestamp)
    except (ValueError, IndexError):
        # Malformed line, skip it
        return None


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """Parse ISO format timestamp string to datetime object."""
    try:
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, AttributeError):
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse Gradle log output to find hanging tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s gradle.log
  %(prog)s gradle.log --min-duration 300
  %(prog)s gradle.log --min-duration 60 --summary
        """
    )
    parser.add_argument("file", type=argparse.FileType("r"), 
                       help="Text file containing Gradle stdout")
    parser.add_argument("--min-duration", type=int, default=0,
                       help="Minimum duration in seconds to report (default: 0, report all)")
    parser.add_argument("--summary", action="store_true",
                       help="Show summary statistics")
    parser.add_argument("--output", type=str,
                       help="Output file to write report (default: stdout)")
    parser.add_argument("--format", type=str, choices=["text", "json"], default="text",
                       help="Output format (default: text)")
    args = parser.parse_args()

    started: Dict[str, Tuple[str, str]] = {}  # test_name -> (line, timestamp)
    last_test_line: Optional[str] = None
    total_tests_processed = 0
    malformed_lines = 0

    for line in args.file.readlines():
        parsed = parse_test_line(line)
        if parsed is None:
            if "Gradle Test Run" in line:
                malformed_lines += 1
            continue
        
        test, status, timestamp = parsed
        total_tests_processed += 1
        last_test_line = line

        if status == "STARTED":
            started[test] = (line, timestamp)
        elif status in ("PASSED", "FAILED", "SKIPPED"):
            # Test finished, remove from started dict
            started.pop(test, None)

    if last_test_line is None:
        print("No test lines found in the log file.", file=sys.stderr)
        sys.exit(1)

    # Parse the last timestamp to calculate durations
    last_parsed = parse_test_line(last_test_line)
    if last_parsed is None:
        print("Could not parse last test line timestamp.", file=sys.stderr)
        sys.exit(1)
    
    _, _, last_timestamp = last_parsed
    last_dt = parse_timestamp(last_timestamp)
    if last_dt is None:
        print(f"Could not parse timestamp: {last_timestamp}", file=sys.stderr)
        sys.exit(1)

    # Filter and sort unfinished tests by duration
    unfinished_tests = []
    for test_name, (line, start_timestamp) in started.items():
        start_dt = parse_timestamp(start_timestamp)
        if start_dt is None:
            continue
        
        duration_seconds = (last_dt - start_dt).total_seconds()
        if duration_seconds >= args.min_duration:
            unfinished_tests.append((test_name, line, start_timestamp, duration_seconds))
    
    # Sort by duration (longest first)
    unfinished_tests.sort(key=lambda x: x[3], reverse=True)

    if len(unfinished_tests) > 0:
        print(f"Found {len(unfinished_tests)} test(s) that were started, but apparently not finished")
        if args.min_duration > 0:
            print(f"(Filtered to tests running for at least {args.min_duration} seconds)")
        print()
    else:
        if args.min_duration > 0:
            print(f"No unfinished tests found running for at least {args.min_duration} seconds.")
        else:
            print("No unfinished tests found.")
        sys.exit(0)

    # Prepare output
    output_lines = []
    
    for test_name, line, start_timestamp, duration_seconds in unfinished_tests:
        output_lines.append("-" * 80)
        output_lines.append(f"Test: {test_name}")
        output_lines.append(f"Duration: {pretty_time_duration(duration_seconds)} ({int(duration_seconds)}s)")
        output_lines.append(f"Started at: {start_timestamp}")
        output_lines.append(f"Raw line: {line.strip()}")

    if args.summary:
        output_lines.append("")
        output_lines.append("=" * 80)
        output_lines.append("Summary:")
        output_lines.append(f"  Total tests processed: {total_tests_processed}")
        output_lines.append(f"  Unfinished tests: {len(unfinished_tests)}")
        if len(unfinished_tests) > 0:
            avg_duration = sum(d for _, _, _, d in unfinished_tests) / len(unfinished_tests)
            max_duration = unfinished_tests[0][3]
            min_duration = unfinished_tests[-1][3]
            output_lines.append(f"  Average duration: {pretty_time_duration(avg_duration)} ({int(avg_duration)}s)")
            output_lines.append(f"  Longest duration: {pretty_time_duration(max_duration)} ({int(max_duration)}s)")
            output_lines.append(f"  Shortest duration: {pretty_time_duration(min_duration)} ({int(min_duration)}s)")
        if malformed_lines > 0:
            output_lines.append(f"  Malformed lines skipped: {malformed_lines}")
    
    # Format and write output
    if args.format == "json":
        import json
        output_data = {
            "unfinished_tests": [
                {
                    "test_name": test_name,
                    "duration_seconds": duration_seconds,
                    "started_at": start_timestamp,
                    "raw_line": line.strip()
                }
                for test_name, line, start_timestamp, duration_seconds in unfinished_tests
            ],
            "summary": {
                "total_tests_processed": total_tests_processed,
                "unfinished_tests_count": len(unfinished_tests),
                "malformed_lines": malformed_lines
            }
        }
        if args.summary and len(unfinished_tests) > 0:
            avg_duration = sum(d for _, _, _, d in unfinished_tests) / len(unfinished_tests)
            output_data["summary"]["average_duration"] = avg_duration
            output_data["summary"]["max_duration"] = unfinished_tests[0][3]
            output_data["summary"]["min_duration"] = unfinished_tests[-1][3]
        output_text = json.dumps(output_data, indent=2)
    else:
        output_text = "\n".join(output_lines)
    
    # Write to file or stdout
    if args.output:
        with open(args.output, "w") as f:
            f.write(output_text)
        print(f"Report written to {args.output}")
    else:
        print(output_text)
