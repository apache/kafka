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
from collections import defaultdict
from datetime import datetime
from typing import Optional, Tuple, Dict, List


def parse_test_result_line(line: str) -> Optional[Tuple[str, str, float]]:
    """
    Parse a Gradle test result line to extract test name, status, and duration.
    
    Returns:
        Tuple of (test_name, status, duration_seconds) if line is valid, None otherwise.
    """
    try:
        if "Gradle Test Run" not in line:
            return None
        
        # Extract timestamp and parse the line structure
        parts = line.strip().split(" ", 1)
        if len(parts) < 2:
            return None
        
        timestamp_str = parts[0]
        rest = parts[1]
        
        # Split by " > " to get test hierarchy
        toks = rest.split(" > ")
        if len(toks) < 3:
            return None
        
        # Last token should be "name STATUS" or "name STATUS duration"
        name_status_duration = toks[-1].rsplit(" ", 2)
        if len(name_status_duration) < 2:
            return None
        
        name = name_status_duration[0]
        status = name_status_duration[1]
        
        # Try to extract duration if present
        duration = 0.0
        if len(name_status_duration) == 3:
            try:
                # Duration might be in format like "1.234s" or "1234ms"
                dur_str = name_status_duration[2]
                if dur_str.endswith("s"):
                    duration = float(dur_str[:-1])
                elif dur_str.endswith("ms"):
                    duration = float(dur_str[:-2]) / 1000.0
                else:
                    duration = float(dur_str)
            except ValueError:
                duration = 0.0
        
        # Reconstruct full test name
        name_toks = toks[2:-1] + [name]
        test_name = " > ".join(name_toks)
        
        return (test_name, status, duration)
    except (ValueError, IndexError):
        # Malformed line, skip it
        return None


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """Parse ISO format timestamp string to datetime object."""
    try:
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, AttributeError):
        return None


def analyze_test_patterns(log_file, min_duration: float = 0.0, status_filter: Optional[str] = None):
    """
    Analyze test patterns from a Gradle log file.
    
    Returns:
        Dictionary with analysis results.
    """
    test_results: List[Tuple[str, str, float]] = []
    failed_tests = []
    slow_tests = []
    total_tests = 0
    malformed_lines = 0
    skipped_tests = 0
    
    for line in log_file.readlines():
        parsed = parse_test_result_line(line)
        if parsed is None:
            if "Gradle Test Run" in line:
                malformed_lines += 1
            continue
        
        test_name, status, duration = parsed
        total_tests += 1
        test_results.append((test_name, status, duration))
        
        if status == "FAILED":
            failed_tests.append((test_name, duration))
        elif status == "SKIPPED":
            skipped_tests += 1
        elif duration >= min_duration:
            slow_tests.append((test_name, status, duration))
    
    # Group by test class/package
    by_class: Dict[str, List[Tuple[str, str, float]]] = defaultdict(list)
    for test_name, status, duration in test_results:
        # Extract class name (everything before the last " > ")
        if " > " in test_name:
            class_name = " > ".join(test_name.split(" > ")[:-1])
        else:
            class_name = "Unknown"
        by_class[class_name].append((test_name, status, duration))
    
    # Calculate statistics
    if test_results:
        durations = [d for _, _, d in test_results if d > 0]
        avg_duration = sum(durations) / len(durations) if durations else 0.0
        max_duration = max(durations) if durations else 0.0
    else:
        avg_duration = 0.0
        max_duration = 0.0
    
    return {
        "total_tests": total_tests,
        "failed_tests": failed_tests,
        "slow_tests": slow_tests,
        "skipped_tests": skipped_tests,
        "malformed_lines": malformed_lines,
        "by_class": dict(by_class),
        "avg_duration": avg_duration,
        "max_duration": max_duration,
        "test_results": test_results
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze test patterns from Gradle log output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s gradle.log
  %(prog)s gradle.log --min-duration 5.0
  %(prog)s gradle.log --status-filter FAILED --output report.txt
        """
    )
    parser.add_argument("file", type=argparse.FileType("r"),
                       help="Text file containing Gradle stdout")
    parser.add_argument("--min-duration", type=float, default=0.0,
                       help="Minimum duration in seconds to report slow tests (default: 0.0)")
    parser.add_argument("--status-filter", type=str, choices=["PASSED", "FAILED", "SKIPPED"],
                       help="Filter tests by status")
    parser.add_argument("--output", type=str,
                       help="Output file to write report (default: stdout)")
    parser.add_argument("--group-by-class", action="store_true",
                       help="Group results by test class")
    args = parser.parse_args()
    
    # Analyze the log file
    results = analyze_test_patterns(args.file, args.min_duration, args.status_filter)
    
    # Prepare output
    output_lines = []
    output_lines.append("=" * 80)
    output_lines.append("Test Pattern Analysis Report")
    output_lines.append("=" * 80)
    output_lines.append(f"\nTotal tests processed: {results['total_tests']}")
    output_lines.append(f"Failed tests: {len(results['failed_tests'])}")
    output_lines.append(f"Skipped tests: {results['skipped_tests']}")
    output_lines.append(f"Slow tests (>= {args.min_duration}s): {len(results['slow_tests'])}")
    output_lines.append(f"Average test duration: {results['avg_duration']:.2f}s")
    output_lines.append(f"Maximum test duration: {results['max_duration']:.2f}s")
    
    if results['malformed_lines'] > 0:
        output_lines.append(f"Malformed lines skipped: {results['malformed_lines']}")
    
    # Filter results if status filter is specified
    filtered_results = results['test_results']
    if args.status_filter:
        filtered_results = [(n, s, d) for n, s, d in filtered_results if s == args.status_filter]
        output_lines.append(f"\nFiltered to {args.status_filter} tests: {len(filtered_results)}")
    
    # Show failed tests
    if results['failed_tests']:
        output_lines.append("\n" + "-" * 80)
        output_lines.append("Failed Tests:")
        output_lines.append("-" * 80)
        for test_name, duration in results['failed_tests']:
            output_lines.append(f"  {test_name} ({duration:.2f}s)")
    
    # Show slow tests
    if results['slow_tests']:
        output_lines.append("\n" + "-" * 80)
        output_lines.append(f"Slow Tests (>= {args.min_duration}s):")
        output_lines.append("-" * 80)
        # Sort by duration descending
        sorted_slow = sorted(results['slow_tests'], key=lambda x: x[2], reverse=True)
        for test_name, status, duration in sorted_slow[:20]:  # Top 20
            output_lines.append(f"  {test_name} [{status}] ({duration:.2f}s)")
        if len(sorted_slow) > 20:
            output_lines.append(f"  ... and {len(sorted_slow) - 20} more")
    
    # Group by class if requested
    if args.group_by_class:
        output_lines.append("\n" + "-" * 80)
        output_lines.append("Tests Grouped by Class:")
        output_lines.append("-" * 80)
        for class_name, tests in sorted(results['by_class'].items()):
            output_lines.append(f"\n{class_name}:")
            for test_name, status, duration in tests[:5]:  # Top 5 per class
                output_lines.append(f"  {test_name} [{status}] ({duration:.2f}s)")
            if len(tests) > 5:
                output_lines.append(f"  ... and {len(tests) - 5} more")
    
    output_lines.append("\n" + "=" * 80)
    
    # Write output
    output_text = "\n".join(output_lines)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output_text)
        print(f"Report written to {args.output}")
    else:
        print(output_text)
    
    # Exit with error code if there are failures
    if results['failed_tests']:
        sys.exit(1)
    sys.exit(0)

