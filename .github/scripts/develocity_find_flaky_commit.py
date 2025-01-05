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

import os
import logging
from datetime import datetime, timedelta
import pytz
from typing import Optional, Dict, Tuple, List
import argparse
from develocity_reports import TestAnalyzer  # Import the TestAnalyzer class

logger = logging.getLogger(__name__)

def find_first_flaky_commit(
    test_class: str,
    test_case: str,
    N: int = 2,
    M: int = 6,
    days_to_check: int = 30
) -> List[Tuple[str, Dict]]:
    """
    Find the first commit that introduced flakiness for a given test.
    
    Args:
        test_class: Full name of the test class
        test_case: Name of the test case/method
        N: Number of flaky/fail occurrences needed in window
        M: Window size to confirm flakiness pattern
        days_to_check: Number of days to look back in history
        
    Returns:
        List of (commit_id, details_dict) or empty list if not found
    """
    # Initialize TestAnalyzer with Develocity credentials
    token = os.environ.get("DEVELOCITY_ACCESS_TOKEN")
    if not token:
        raise ValueError("DEVELOCITY_ACCESS_TOKEN environment variable must be set")
    
    analyzer = TestAnalyzer("https://ge.apache.org", token)
    
    try:
        # Calculate time range
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(days=days_to_check)

        analyzer.clear_cache()
        
        # Get test execution history
        test_results = analyzer.get_test_case_details(
            container_name=test_class,
            project="kafka",
            chunk_start=start_time,
            chunk_end=end_time,
            test_type="test",
            include_passed=True
        )
        
        if not test_results:
            logger.warning(f"No test results found for {test_class}")
            return []
        
        # Print all test case names
        logger.info("Found test cases:")
        for result in test_results:
            logger.info(f"  - {result.name}")

        # Construct full test case name by combining test class and test case
        test_case = f"{test_class}.{test_case}"
        logger.info(f"Looking for test case: {test_case}")
        
        # Filter for specific test case
        matching_results = [r for r in test_results if r.name.endswith(test_case)]
        if not matching_results:
            logger.warning(f"No results found for test case {test_case}")
            return []
        
        # Get timeline entries sorted by timestamp
        timeline = []
        for result in matching_results:
            timeline.extend(result.timeline)
        
        timeline.sort(key=lambda x: x.timestamp)
        
        # Print all test executions in chronological order with commit IDs
        logger.info("\nComplete test execution history:")
        builds_info = {}
        build_ids = [entry.build_id for entry in timeline]
        builds_info = analyzer.get_build_info(build_ids, "kafka", "test", days_to_check)
        
        for entry in timeline:
            build_info = builds_info.get(entry.build_id)
            commit_id = build_info.git_commit if build_info else "unknown"
            logger.info(f"Timestamp: {entry.timestamp.isoformat()} - Build ID: {entry.build_id} - Commit: {commit_id} - Status: {entry.outcome.upper()}")

        # Get all build info once at the beginning
        all_build_ids = set(entry.build_id for entry in timeline)
        builds_info = analyzer.get_build_info(list(all_build_ids), "kafka", "test", days_to_check)
        
        # Convert timeline to (build_id, commit, status) tuples using cached build info
        test_history = [
            (
                entry.build_id,
                builds_info.get(entry.build_id).git_commit if builds_info.get(entry.build_id) else "unknown",
                entry.outcome.upper()
            ) 
            for entry in timeline
        ]
        
        logger.info(f"\nAnalyzing {len(test_history)} test executions")
        
        # Initialize list to store all potential flaky patterns
        flaky_patterns = []
        
        # Find all flaky commit patterns using cached build info
        for i in range(len(test_history)):
            build_id, commit, status = test_history[i]
            
            if status in ("FLAKY", "FAILED"):
                window_end = min(i + M, len(test_history))
                window_data = test_history[i:window_end]
                flaky_count = sum(1 for _, _, s in window_data if s in ("FLAKY", "FAILED"))
                
                if flaky_count >= N:
                    build_info = builds_info.get(build_id)
                    if build_info:
                        details = {
                            'build_id': build_id,
                            'git_commit': build_info.git_commit,
                            'timestamp': build_info.timestamp,
                            'duration': build_info.duration,
                            'failed': build_info.has_failed,
                            'window_data': [
                                {
                                    'build_id': bid,
                                    'git_commit': commit,
                                    'status': status,
                                    'timestamp': builds_info.get(bid).timestamp  # Use cached build info
                                }
                                for bid, commit, status in window_data
                            ]
                        }
                        flaky_patterns.append((build_info.git_commit, details))
        
        return flaky_patterns
        
    except Exception as e:
        logger.error(f"Error finding flaky commit: {str(e)}")
        return []

def main():
    # Hardcoded test values
    test_class = "kafka.api.PlaintextAdminIntegrationTest"
    test_case = "testConsumerGroups(String, String)[2]"
    occurrences = 3
    window = 5
    days = 30
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler("flaky_commit.log")]
    )
    
    # Find flaky commits
    flaky_patterns = find_first_flaky_commit(
        test_class,
        test_case,
        occurrences,
        window,
        days
    )
    
    # Print results
    if flaky_patterns:
        print("\nPotential flaky commit patterns found:")
        for commit, details in flaky_patterns:
            print(f"\nGit Commit: {commit}")
            print(f"Build ID: {details['build_id']}")
            print(f"Timestamp: {details['timestamp'].isoformat()}")
            print(f"Build Failed: {details['failed']}")
            print("\nWindow data:")
            for entry in details['window_data']:
                print(f"  {entry['timestamp'].isoformat()} - {entry['status']}")
                print(f"    Build ID: {entry['build_id']}")
                print(f"    Git Commit: {entry['git_commit']}")
    else:
        print("\nNo flaky commit patterns found")

if __name__ == "__main__":
    main()