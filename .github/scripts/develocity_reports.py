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
import requests
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import pytz  # Add this import for timezone handling
from collections import defaultdict
import time
import logging
import concurrent.futures  # Add this import at the top
import pathlib
import pickle
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

@dataclass
class TestOutcome:
    passed: int
    failed: int 
    skipped: int
    flaky: int
    not_selected: int = field(metadata={'name': 'notSelected'})
    total: int

@dataclass
class BuildInfo:
    id: str
    timestamp: datetime
    duration: int
    has_failed: bool

@dataclass
class TestTimelineEntry:
    build_id: str
    timestamp: datetime
    outcome: str  # "passed", "failed", "flaky", etc.

@dataclass
class TestResult:
    name: str
    outcome_distribution: TestOutcome
    first_seen: datetime
    timeline: List[TestTimelineEntry] = field(default_factory=list)
    recent_failure_rate: float = 0.0  # Added to track recent failure trends

@dataclass
class TestContainerResult:
    build_id: str
    outcome: str
    timestamp: Optional[datetime] = None

@dataclass
class TestCaseResult(TestResult):
    """Extends TestResult to include container-specific information"""
    container_name: str = ""
    
@dataclass
class BuildCache:
    last_update: datetime
    builds: Dict[str, 'BuildInfo']
    
    def to_dict(self):
        return {
            'last_update': self.last_update.isoformat(),
            'builds': {k: asdict(v) for k, v in self.builds.items()}
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'BuildCache':
        return cls(
            last_update=datetime.fromisoformat(data['last_update']),
            builds={k: BuildInfo(**v) for k, v in data['builds'].items()}
        )

class CacheProvider(ABC):
    @abstractmethod
    def get_cache(self) -> Optional[BuildCache]:
        pass
    
    @abstractmethod
    def save_cache(self, cache: BuildCache):
        pass

class LocalCacheProvider(CacheProvider):
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.path.join(os.path.expanduser("~"), ".develocity_cache")
        self.cache_file = os.path.join(cache_dir, "build_cache.pkl")
        os.makedirs(cache_dir, exist_ok=True)
    
    def get_cache(self) -> Optional[BuildCache]:
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.warning(f"Failed to load local cache: {e}")
        return None
    
    def save_cache(self, cache: BuildCache):
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(cache, f)
        except Exception as e:
            logger.warning(f"Failed to save local cache: {e}")

class GitHubActionsCacheProvider(CacheProvider):
    def __init__(self):
        self.cache_key = "develocity-build-cache"
    
    def get_cache(self) -> Optional[BuildCache]:
        try:
            # Check if running in GitHub Actions
            if not os.environ.get('GITHUB_ACTIONS'):
                return None
                
            cache_path = os.environ.get('GITHUB_WORKSPACE', '')
            cache_file = os.path.join(cache_path, self.cache_key + '.json')
            
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    return BuildCache.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load GitHub Actions cache: {e}")
        return None
    
    def save_cache(self, cache: BuildCache):
        try:
            if not os.environ.get('GITHUB_ACTIONS'):
                return
                
            cache_path = os.environ.get('GITHUB_WORKSPACE', '')
            cache_file = os.path.join(cache_path, self.cache_key + '.json')
            
            with open(cache_file, 'w') as f:
                json.dump(cache.to_dict(), f)
        except Exception as e:
            logger.warning(f"Failed to save GitHub Actions cache: {e}")

class TestAnalyzer:
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url
        self.headers = {
            'Authorization': f'Bearer {auth_token}',
            'Accept': 'application/json'
        }
        self.default_chunk_size = timedelta(days=14)
        self.api_retry_delay = 2  # seconds
        self.max_api_retries = 3
        
        # Initialize cache providers
        self.cache_providers = [
            GitHubActionsCacheProvider(),
            LocalCacheProvider()
        ]
        self.build_cache = None
        self._load_cache()
    
    def _load_cache(self):
        """Load cache from the first available provider"""
        for provider in self.cache_providers:
            cache = provider.get_cache()
            if cache is not None:
                self.build_cache = cache
                logger.info(f"Loaded cache from {provider.__class__.__name__}")
                return
        logger.info("No existing cache found")
    
    def _save_cache(self):
        """Save cache to all providers"""
        if self.build_cache:
            for provider in self.cache_providers:
                provider.save_cache(self.build_cache)
                logger.info(f"Saved cache to {provider.__class__.__name__}")

    def build_query(self, project: str, chunk_start: datetime, chunk_end: datetime, test_type: str) -> str:
        """
        Constructs the query string to be used in both build info and test containers API calls.
        
        Args:
            project: The project name.
            chunk_start: The start datetime for the chunk.
            chunk_end: The end datetime for the chunk.
            test_type: The type of tests to query.
        
        Returns:
            A formatted query string.
        """
        return f'project:{project} buildStartTime:[{chunk_start.isoformat()} TO {chunk_end.isoformat()}] gradle.requestedTasks:{test_type}'
    
    def process_chunk(
        self,
        chunk_start: datetime,
        chunk_end: datetime,
        project: str,
        test_type: str,
        remaining_build_ids: set,
        max_builds_per_request: int
    ) -> Dict[str, BuildInfo]:
        """Helper method to process a single chunk of build information"""
        chunk_builds = {}
        
        # Use the helper method to build the query
        query = self.build_query(project, chunk_start, chunk_end, test_type)
        
        # Initialize pagination for this chunk
        from_build = None
        continue_chunk = True

        while continue_chunk and remaining_build_ids:
            query_params = {
                'query': query,
                'models': ['gradle-attributes'],
                'allModels': 'false',
                'maxBuilds': max_builds_per_request,
                'reverse': 'false',
                'fromInstant': int(chunk_start.timestamp() * 1000)
            }
            
            if from_build:
                query_params['fromBuild'] = from_build
            
            for attempt in range(self.max_api_retries):
                try:
                    response = requests.get(
                        f'{self.base_url}/api/builds',
                        headers=self.headers,
                        params=query_params,
                        timeout=(5, 30)
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.Timeout:
                    if attempt == self.max_api_retries - 1:
                        raise
                    time.sleep(self.api_retry_delay * (attempt + 1))
                except requests.exceptions.RequestException:
                    raise

            response_json = response.json()
            
            if not response_json:
                break
                
            for build in response_json:
                build_id = build['id']
                
                if 'models' in build and 'gradleAttributes' in build['models']:
                    gradle_attrs = build['models']['gradleAttributes']
                    if 'model' in gradle_attrs:
                        attrs = gradle_attrs['model']
                        build_timestamp = datetime.fromtimestamp(attrs['buildStartTime'] / 1000, pytz.UTC)
                        
                        if build_timestamp >= chunk_end:
                            continue_chunk = False
                            break
                        
                        if build_id in remaining_build_ids:
                            if 'problem' not in gradle_attrs:
                                chunk_builds[build_id] = BuildInfo(
                                    id=build_id,
                                    timestamp=build_timestamp,
                                    duration=attrs.get('buildDuration'),
                                    has_failed=attrs.get('hasFailed', False)
                                )
            
            if continue_chunk and response_json:
                from_build = response_json[-1]['id']
            else:
                continue_chunk = False
            
            time.sleep(0.5)  # Rate limiting between pagination requests
            
        return chunk_builds

    def get_build_info(self, build_ids: List[str], project: str, test_type: str, query_days: int) -> Dict[str, BuildInfo]:
        builds = {}
        max_builds_per_request = 100
        cutoff_date = datetime.now(pytz.UTC) - timedelta(days=query_days)
        
        # Get builds from cache if available
        if self.build_cache:
            cached_builds = self.build_cache.builds
            cached_cutoff = self.build_cache.last_update - timedelta(days=query_days)
            
            # Use cached data for builds within the cache period
            for build_id in build_ids:
                if build_id in cached_builds:
                    build = cached_builds[build_id]
                    if build.timestamp >= cached_cutoff:
                        builds[build_id] = build
            
            # Update cutoff date to only fetch new data
            cutoff_date = self.build_cache.last_update
            logger.info(f"Using cached data up to {cutoff_date.isoformat()}")
            
            # Remove already found builds from the search list
            build_ids = [bid for bid in build_ids if bid not in builds]
        
        if not build_ids:
            logger.info("All builds found in cache")
            return builds
        
        # Fetch remaining builds from API
        remaining_build_ids = set(build_ids)
        current_time = datetime.now(pytz.UTC)
        chunk_size = self.default_chunk_size

        # Create time chunks
        chunks = []
        chunk_start = cutoff_date
        while chunk_start < current_time:
            chunk_end = min(chunk_start + chunk_size, current_time)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        total_start_time = time.time()

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_chunk = {
                executor.submit(
                    self.process_chunk, 
                    chunk[0], 
                    chunk[1], 
                    project, 
                    test_type, 
                    remaining_build_ids.copy(),
                    max_builds_per_request
                ): chunk for chunk in chunks
            }

            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    chunk_builds = future.result()
                    builds.update(chunk_builds)
                    remaining_build_ids -= set(chunk_builds.keys())
                except Exception as e:
                    logger.error(f"Chunk processing generated an exception: {str(e)}")

        total_duration = time.time() - total_start_time
        logger.info(
            f"\nBuild Info Performance:"
            f"\n  Total Duration: {total_duration:.2f}s"
            f"\n  Builds Retrieved: {len(builds)}"
            f"\n  Builds Not Found: {len(remaining_build_ids)}"
        )
        
        # Update cache with new data
        if builds:
            if not self.build_cache:
                self.build_cache = BuildCache(current_time, {})
            self.build_cache.builds.update(builds)
            self.build_cache.last_update = current_time
            self._save_cache()
        
        return builds

    def get_test_results(self, project: str, threshold_days: int, test_type: str = "quarantinedTest",
                        outcomes: List[str] = None) -> List[TestResult]:
        """Fetch test results with timeline information"""
        if outcomes is None:
            outcomes = ["failed", "flaky"]

        logger.debug(f"Fetching test results for project {project}, last {threshold_days} days")
        
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(days=threshold_days)
        
        all_results = {}
        build_ids = set()
        test_container_results = defaultdict(list)
        
        chunk_size = self.default_chunk_size
        chunk_start = start_time
        
        while chunk_start < end_time:
            chunk_end = min(chunk_start + chunk_size, end_time)
            logger.debug(f"Processing chunk: {chunk_start} to {chunk_end}")
            
            # Use the helper method to build the query
            query = self.build_query(project, chunk_start, chunk_end, test_type)
            
            query_params = {
                'query': query,
                'testOutcomes': outcomes,
                'container': '*',
                'include': ['buildScanIds']  # Explicitly request build scan IDs
            }

            response = requests.get(
                f'{self.base_url}/api/tests/containers',
                headers=self.headers,
                params=query_params
            )
            response.raise_for_status()
            
            for test in response.json()['content']:
                test_name = test['name']
                logger.debug(f"Processing test: {test_name}")
                
                if test_name not in all_results:
                    outcome_data = test['outcomeDistribution']
                    if 'notSelected' in outcome_data:
                        outcome_data['not_selected'] = outcome_data.pop('notSelected')
                    outcome = TestOutcome(**outcome_data)
                    all_results[test_name] = TestResult(test_name, outcome, chunk_start)
                
                # Collect build IDs by outcome
                if 'buildScanIdsByOutcome' in test:
                    scan_ids = test['buildScanIdsByOutcome']
                    
                    for outcome, ids in scan_ids.items():
                        if ids:  # Only process if we have IDs
                            for build_id in ids:
                                build_ids.add(build_id)
                                test_container_results[test_name].append(
                                    TestContainerResult(build_id=build_id, outcome=outcome)
                                )
            
            chunk_start = chunk_end

        logger.debug(f"Total unique build IDs collected: {len(build_ids)}")
        
        # Fetch build information using the updated get_build_info method
        builds = self.get_build_info(list(build_ids), project, test_type, threshold_days)
        logger.debug(f"Retrieved {len(builds)} builds from API")
        logger.debug(f"Retrieved build IDs: {sorted(builds.keys())}")

        # Update test results with timeline information
        for test_name, result in all_results.items():
            logger.debug(f"\nProcessing timeline for test: {test_name}")
            timeline = []
            for container_result in test_container_results[test_name]:
                logger.debug(f"Processing container result: {container_result}")
                if container_result.build_id in builds:
                    build_info = builds[container_result.build_id]
                    timeline.append(TestTimelineEntry(
                        build_id=container_result.build_id,
                        timestamp=build_info.timestamp,
                        outcome=container_result.outcome
                    ))
                else:
                    logger.warning(f"Build ID {container_result.build_id} not found in builds response")
            
            # Sort timeline by timestamp
            result.timeline = sorted(timeline, key=lambda x: x.timestamp)
            logger.debug(f"Final timeline entries for {test_name}: {len(result.timeline)}")
            
            # Calculate recent failure rate
            recent_cutoff = datetime.now(pytz.UTC) - timedelta(days=30)
            recent_runs = [t for t in timeline if t.timestamp >= recent_cutoff]
            if recent_runs:
                recent_failures = sum(1 for t in recent_runs if t.outcome in ('failed', 'flaky'))
                result.recent_failure_rate = recent_failures / len(recent_runs)

        return list(all_results.values())

    def get_defective_tests(self, results: List[TestResult]) -> Dict[str, TestResult]:
        """
        Analyze test results to find defective tests (failed or flaky)
        """
        defective_tests = {}
        
        for result in results:
            if result.outcome_distribution.failed > 0 or result.outcome_distribution.flaky > 0:
                defective_tests[result.name] = result
                
        return defective_tests

    def get_long_quarantined_tests(self, results: List[TestResult], quarantine_threshold_days: int = 60) -> Dict[str, TestResult]:
        """
        Find tests that have been quarantined longer than the threshold.
        These are candidates for removal or rewriting.
        
        Args:
            results: List of test results
            quarantine_threshold_days: Number of days after which a quarantined test should be considered for removal/rewrite
        """
        long_quarantined = {}
        current_time = datetime.now(pytz.UTC)
        
        for result in results:
            days_quarantined = (current_time - result.first_seen).days
            if days_quarantined >= quarantine_threshold_days:
                long_quarantined[result.name] = (result, days_quarantined)
        
        return long_quarantined

    def get_problematic_quarantined_tests(
        self,
        results: List[TestResult],
        quarantine_threshold_days: int = 60,
        min_failure_rate: float = 0.3,
        recent_failure_threshold: float = 0.5
    ) -> Dict[str, Dict]:
        """Enhanced version that includes test case details"""
        problematic_tests = {}
        current_time = datetime.now(pytz.UTC)
        chunk_start = current_time - timedelta(days=7)  # Last 7 days for test cases
        
        for result in results:
            days_quarantined = (current_time - result.first_seen).days
            if days_quarantined >= quarantine_threshold_days:
                total_runs = result.outcome_distribution.total
                if total_runs > 0:
                    problem_runs = result.outcome_distribution.failed + result.outcome_distribution.flaky
                    failure_rate = problem_runs / total_runs
                    
                    if failure_rate >= min_failure_rate or result.recent_failure_rate >= recent_failure_threshold:
                        # Get detailed test case information
                        try:
                            test_cases = self.get_test_case_details(
                                result.name, 
                                "kafka", 
                                chunk_start,
                                current_time,
                                test_type="quarantinedTest"
                            )
                            
                            problematic_tests[result.name] = {
                                'container_result': result,
                                'days_quarantined': days_quarantined,
                                'failure_rate': failure_rate,
                                'recent_failure_rate': result.recent_failure_rate,
                                'test_cases': test_cases
                            }
                        except Exception as e:
                            logger.error(f"Error getting test case details for {result.name}: {str(e)}")
        
        return problematic_tests

    def get_test_case_details(
        self,
        container_name: str,
        project: str,
        chunk_start: datetime,
        chunk_end: datetime,
        test_type: str = "quarantinedTest"
    ) -> List[TestCaseResult]:
        """
        Fetch detailed test case results for a specific container.
        
        Args:
            container_name: Name of the test container
            project: The project name
            chunk_start: Start time for the query
            chunk_end: End time for the query
            test_type: Type of tests to query (default: "quarantinedTest")
        """
        # Use the helper method to build the query, similar to get_test_results
        query = self.build_query(project, chunk_start, chunk_end, test_type)
        
        query_params = {
            'query': query,
            'testOutcomes': ['failed', 'flaky'],
            'container': container_name,
            'include': ['buildScanIds'],  # Explicitly request build scan IDs
            'limit': 1000
        }

        try:
            response = requests.get(
                f'{self.base_url}/api/tests/cases',
                headers=self.headers,
                params=query_params
            )
            response.raise_for_status()
            
            test_cases = []
            content = response.json().get('content', [])
            
            # Collect all build IDs first
            build_ids = set()
            for test in content:
                if 'buildScanIdsByOutcome' in test:
                    for outcome_type, ids in test['buildScanIdsByOutcome'].items():
                        build_ids.update(ids)
            
            # Get build info for all build IDs
            builds = self.get_build_info(list(build_ids), project, test_type, 7)  # 7 days for test cases
            
            for test in content:
                outcome_data = test['outcomeDistribution']
                if 'notSelected' in outcome_data:
                    outcome_data['not_selected'] = outcome_data.pop('notSelected')
                outcome = TestOutcome(**outcome_data)
                
                test_case = TestCaseResult(
                    name=test['name'],
                    outcome_distribution=outcome,
                    first_seen=chunk_start,
                    container_name=container_name
                )
                
                # Add build information with proper timestamps
                if 'buildScanIdsByOutcome' in test:
                    for outcome_type, build_ids in test['buildScanIdsByOutcome'].items():
                        for build_id in build_ids:
                            if build_id in builds:
                                build_info = builds[build_id]
                                test_case.timeline.append(
                                    TestTimelineEntry(
                                        build_id=build_id,
                                        timestamp=build_info.timestamp,
                                        outcome=outcome_type
                                    )
                                )
                            else:
                                logger.warning(f"Build ID {build_id} not found for test case {test['name']}")
                
                # Sort timeline by timestamp
                test_case.timeline.sort(key=lambda x: x.timestamp)
                test_cases.append(test_case)
            
            return test_cases
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching test case details for {container_name}: {str(e)}")
            raise

    def get_flaky_test_regressions(self, project: str, results: List[TestResult], 
                                 recent_days: int = 7, min_flaky_rate: float = 0.2) -> Dict[str, Dict]:
        """
        Identify tests that have recently started showing flaky behavior.
        
        Args:
            project: The project name
            results: List of test results
            recent_days: Number of days to consider for recent behavior
            min_flaky_rate: Minimum flaky rate to consider a test as problematic
        """
        flaky_regressions = {}
        current_time = datetime.now(pytz.UTC)
        recent_cutoff = current_time - timedelta(days=recent_days)
        
        for result in results:
            # Skip tests with no timeline data
            if not result.timeline:
                continue
                
            # Split timeline into recent and historical periods
            recent_entries = [t for t in result.timeline if t.timestamp >= recent_cutoff]
            historical_entries = [t for t in result.timeline if t.timestamp < recent_cutoff]
            
            if not recent_entries or not historical_entries:
                continue
            
            # Calculate flaky rates
            recent_flaky = sum(1 for t in recent_entries if t.outcome == 'flaky')
            recent_total = len(recent_entries)
            recent_flaky_rate = recent_flaky / recent_total if recent_total > 0 else 0
            
            historical_flaky = sum(1 for t in historical_entries if t.outcome == 'flaky')
            historical_total = len(historical_entries)
            historical_flaky_rate = historical_flaky / historical_total if historical_total > 0 else 0
            
            # Check if there's a significant increase in flakiness
            if recent_flaky_rate >= min_flaky_rate and recent_flaky_rate > historical_flaky_rate * 1.5:
                flaky_regressions[result.name] = {
                    'result': result,
                    'recent_flaky_rate': recent_flaky_rate,
                    'historical_flaky_rate': historical_flaky_rate,
                    'recent_executions': recent_entries,
                    'historical_executions': historical_entries
                }
        
        return flaky_regressions

    def get_cleared_tests(self, project: str, results: List[TestResult], 
                         success_threshold: float = 0.7, min_executions: int = 5) -> Dict[str, Dict]:
        """
        Identify quarantined tests that are consistently passing and could be cleared.
        
        Args:
            project: The project name
            results: List of test results
            success_threshold: Required percentage of successful builds to be considered cleared
            min_executions: Minimum number of executions required to make a determination
        """
        cleared_tests = {}
        current_time = datetime.now(pytz.UTC)
        chunk_start = current_time - timedelta(days=7)  # Last 7 days for test cases
        
        for result in results:
            # Only consider tests with sufficient recent executions
            recent_executions = result.timeline
            if len(recent_executions) < min_executions:
                continue
            
            # Calculate success rate at class level
            successful_runs = sum(1 for t in recent_executions 
                                if t.outcome == 'passed')
            success_rate = successful_runs / len(recent_executions)
            
            # Check if the test meets clearing criteria at class level
            if success_rate >= success_threshold:
                # Verify no recent failures or flaky behavior
                has_recent_issues = any(t.outcome in ['failed', 'flaky'] 
                                     for t in recent_executions[-min_executions:])
                
                if not has_recent_issues:
                    try:
                        # Get test case details
                        test_cases = self.get_test_case_details(
                            result.name,
                            project,
                            chunk_start,
                            current_time,
                            test_type="quarantinedTest"
                        )
                        
                        # Only include if all test cases are also passing consistently
                        all_cases_passing = True
                        passing_test_cases = []
                        
                        for test_case in test_cases:
                            case_total = test_case.outcome_distribution.total
                            if case_total >= min_executions:
                                case_success_rate = test_case.outcome_distribution.passed / case_total
                                
                                # Check recent executions for the test case
                                recent_case_issues = any(t.outcome in ['failed', 'flaky']
                                                       for t in test_case.timeline[-min_executions:])
                                
                                if case_success_rate >= success_threshold and not recent_case_issues:
                                    passing_test_cases.append({
                                        'name': test_case.name,
                                        'success_rate': case_success_rate,
                                        'total_executions': case_total,
                                        'recent_executions': sorted(test_case.timeline, 
                                                               key=lambda x: x.timestamp)[-min_executions:]
                                    })
                                else:
                                    all_cases_passing = False
                                    break
                        
                        if all_cases_passing and passing_test_cases:
                            cleared_tests[result.name] = {
                                'result': result,
                                'success_rate': success_rate,
                                'total_executions': len(recent_executions),
                                'successful_runs': successful_runs,
                                'recent_executions': recent_executions[-min_executions:],
                                'test_cases': passing_test_cases
                            }
                            
                    except Exception as e:
                        logger.error(f"Error getting test case details for {result.name}: {str(e)}")
        
        return cleared_tests

def get_develocity_class_link(class_name: str, threshold_days: int) -> str:
    """
    Generate Develocity link for a test class
    
    Args:
        class_name: Name of the test class
        threshold_days: Number of days to look back in search
    """
    base_url = "https://ge.apache.org/scans/tests"
    params = {
        "search.rootProjectNames": "kafka",
        "search.tags": "github,trunk",
        "search.timeZoneId": "America/New_York",
        "search.relativeStartTime": f"P{threshold_days}D",
        "tests.container": class_name
    }
    return f"{base_url}?{'&'.join(f'{k}={requests.utils.quote(str(v))}' for k, v in params.items())}"

def get_develocity_method_link(class_name: str, method_name: str, threshold_days: int) -> str:
    """
    Generate Develocity link for a test method
    
    Args:
        class_name: Name of the test class
        method_name: Name of the test method
        threshold_days: Number of days to look back in search
    """
    base_url = "https://ge.apache.org/scans/tests"
    
    # Extract just the method name without the class prefix
    if '.' in method_name:
        method_name = method_name.split('.')[-1]
        
    params = {
        "search.rootProjectNames": "kafka",
        "search.tags": "github,trunk",
        "search.timeZoneId": "America/New_York",
        "search.relativeStartTime": f"P{threshold_days}D",
        "tests.container": class_name,
        "tests.test": method_name
    }
    return f"{base_url}?{'&'.join(f'{k}={requests.utils.quote(str(v))}' for k, v in params.items())}"

def print_most_problematic_tests(problematic_tests: Dict[str, Dict], threshold_days: int):
    """Print a summary of the most problematic tests"""
    print("\n## Most Problematic Tests")
    if not problematic_tests:
        print("No high-priority problematic tests found.")
        return
        
    print(f"Found {len(problematic_tests)} tests that have been quarantined for {threshold_days} days and are still failing frequently.")
    
    # Print table with class and method information
    print("\n<table>")
    print("<tr><td>Class</td><td>Test Case</td><td>Failure Rate</td><td>Build Scans</td><td>Link</td></tr>")
    
    for test_name, details in sorted(problematic_tests.items(), 
                                   key=lambda x: x[1]['failure_rate'],
                                   reverse=True):
        class_link = get_develocity_class_link(test_name, threshold_days)
        print(f"<tr><td colspan=\"4\">{test_name}</td><td><a href=\"{class_link}\">↗️</a></td></tr>")
        
        for test_case in sorted(details['test_cases'],
                              key=lambda x: (x.outcome_distribution.failed + x.outcome_distribution.flaky) / x.outcome_distribution.total 
                              if x.outcome_distribution.total > 0 else 0,
                              reverse=True):
            method_name = test_case.name.split('.')[-1]
            if method_name != 'N/A':
                method_link = get_develocity_method_link(test_name, test_case.name, threshold_days)
                total_runs = test_case.outcome_distribution.total
                failure_rate = (test_case.outcome_distribution.failed + test_case.outcome_distribution.flaky) / total_runs if total_runs > 0 else 0
                print(f"<tr><td></td><td>{method_name}</td>"
                      f"<td>{failure_rate:.2%}</td><td>{total_runs}</td>"
                      f"<td><a href=\"{method_link}\">↗️</a></td></tr>")
    print("</table>")
    
    # Print detailed execution history
    print("\n<details>")
    print("<summary>Detailed Execution History</summary>\n")
    
    for test_name, details in sorted(problematic_tests.items(),
                                   key=lambda x: x[1]['failure_rate'],
                                   reverse=True):
        print(f"\n### {test_name}")
        print(f"* Days Quarantined: {details['days_quarantined']}")
        print(f"* Recent Failure Rate: {details['recent_failure_rate']:.2%}")
        print(f"* Total Runs: {details['container_result'].outcome_distribution.total}")
        print(f"* Build Outcomes: Passed: {details['container_result'].outcome_distribution.passed} | "
              f"Failed: {details['container_result'].outcome_distribution.failed} | "
              f"Flaky: {details['container_result'].outcome_distribution.flaky}")
        
        for test_method in sorted(details['test_cases'],
                                key=lambda x: (x.outcome_distribution.failed + x.outcome_distribution.flaky) / x.outcome_distribution.total 
                                if x.outcome_distribution.total > 0 else 0,
                                reverse=True):
            method_name = test_method.name.split('.')[-1]
            if test_method.timeline:
                print(f"\n#### {method_name}")
                print("Recent Executions:")
                print("```")
                print("Date/Time (UTC)      Outcome    Build ID")
                print("-" * 44)
                for entry in sorted(test_method.timeline, key=lambda x: x.timestamp)[-5:]:
                    date_str = entry.timestamp.strftime('%Y-%m-%d %H:%M')
                    print(f"{date_str:<17} {entry.outcome:<10} {entry.build_id}")
                print("```")
    
    print("</details>")

def print_flaky_regressions(flaky_regressions: Dict[str, Dict], threshold_days: int):
    """Print tests that have recently started showing flaky behavior"""
    print("\n## Flaky Test Regressions")
    if not flaky_regressions:
        print("No flaky test regressions found.")
        return
        
    print(f"Found {len(flaky_regressions)} tests that have started showing increased flaky behavior recently.")
    
    # Print table with test details
    print("\n<table>")
    print("<tr><td>Test Class</td><td>Recent Flaky Rate</td><td>Historical Rate</td><td>Recent Executions</td><td>Link</td></tr>")
    
    for test_name, details in flaky_regressions.items():
        class_link = get_develocity_class_link(test_name, threshold_days)
        print(f"<tr><td colspan=\"4\">{test_name}</td><td><a href=\"{class_link}\">↗️</a></td></tr>")
        print(f"<tr><td></td><td>{details['recent_flaky_rate']:.2%}</td>"
              f"<td>{details['historical_flaky_rate']:.2%}</td>"
              f"<td>{len(details['recent_executions'])}</td><td></td></tr>")
        
        # Add recent execution details in sub-rows
        print("<tr><td colspan=\"5\">Recent Executions:</td></tr>")
        for entry in sorted(details['recent_executions'], key=lambda x: x.timestamp)[-5:]:
            date_str = entry.timestamp.strftime('%Y-%m-%d %H:%M')
            print(f"<tr><td></td><td colspan=\"4\">{date_str} - {entry.outcome}</td></tr>")
    print("</table>")
    
    # Print detailed history
    print("\n<details>")
    print("<summary>Detailed Execution History</summary>\n")
    
    for test_name, details in sorted(flaky_regressions.items(),
                                   key=lambda x: x[1]['recent_flaky_rate'],
                                   reverse=True):
        print(f"\n### {test_name}")
        print(f"* Recent Flaky Rate: {details['recent_flaky_rate']:.2%}")
        print(f"* Historical Flaky Rate: {details['historical_flaky_rate']:.2%}")
        print("\nRecent Executions:")
        print("```")
        print("Date/Time (UTC)      Outcome    Build ID")
        print("-" * 44)
        for entry in sorted(details['recent_executions'], key=lambda x: x.timestamp)[-5:]:
            date_str = entry.timestamp.strftime('%Y-%m-%d %H:%M')
            print(f"{date_str:<17} {entry.outcome:<10} {entry.build_id}")
        print("```")
    
    print("</details>")

def print_cleared_tests(cleared_tests: Dict[str, Dict], threshold_days: int):
    """Print tests that are ready to be unquarantined"""
    print("\n## Cleared Tests (Ready for Unquarantine)")
    if not cleared_tests:
        print("No tests ready to be cleared from quarantine.")
        return
        
    # Calculate total number of test methods
    total_methods = sum(len(details['test_cases']) for details in cleared_tests.values())
    
    print(f"Found {len(cleared_tests)} test classes with {total_methods} test methods that have been consistently passing. "
          f"These tests could be candidates for removing quarantine annotations at either class or method level.")
    
    # Print table with class and method information
    print("\n<table>")
    print("<tr><td>Test Class</td><td>Test Method</td><td>Success Rate</td><td>Total Runs</td><td>Recent Status</td><td>Link</td></tr>")
    
    for test_name, details in sorted(cleared_tests.items(),
                                   key=lambda x: x[1]['success_rate'],
                                   reverse=True):
        class_link = get_develocity_class_link(test_name, threshold_days)
        print(f"<tr><td colspan=\"5\">{test_name}</td><td><a href=\"{class_link}\">↗️</a></td></tr>")
        print(f"<tr><td></td><td>Class Overall</td>"
              f"<td>{details['success_rate']:.2%}</td>"
              f"<td>{details['total_executions']}</td>"
              f"<td>{details['successful_runs']} passed</td><td></td></tr>")
        
        for test_case in details['test_cases']:
            method_name = test_case['name'].split('.')[-1]
            method_link = get_develocity_method_link(test_name, test_case['name'], threshold_days)
            recent_status = "N/A"
            if test_case['recent_executions']:
                recent_status = test_case['recent_executions'][-1].outcome
            
            print(f"<tr><td></td><td>{method_name}</td>"
                  f"<td>{test_case['success_rate']:.2%}</td>"
                  f"<td>{test_case['total_executions']}</td>"
                  f"<td>{recent_status}</td>"
                  f"<td><a href=\"{method_link}\">↗️</a></td></tr>")
        print("<tr><td colspan=\"6\">&nbsp;</td></tr>")
    print("</table>")
    
    # Print detailed history
    print("\n<details>")
    print("<summary>Detailed Test Method History</summary>\n")
    
    for test_name, details in sorted(cleared_tests.items(),
                                   key=lambda x: x[1]['success_rate'],
                                   reverse=True):
        print(f"\n### {test_name}")
        print(f"* Overall Success Rate: {details['success_rate']:.2%}")
        print(f"* Total Executions: {details['total_executions']}")
        print(f"* Consecutive Successful Runs: {details['successful_runs']}")
        
        for test_case in details['test_cases']:
            method_name = test_case['name'].split('.')[-1]
            print(f"\n#### {method_name}")
            print(f"* Success Rate: {test_case['success_rate']:.2%}")
            print(f"* Total Runs: {test_case['total_executions']}")
            print("\nRecent Executions:")
            print("```")
            print("Date/Time (UTC)      Outcome    Build ID")
            print("-" * 44)
            for entry in test_case['recent_executions']:
                date_str = entry.timestamp.strftime('%Y-%m-%d %H:%M')
                print(f"{date_str:<17} {entry.outcome:<10} {entry.build_id}")
            print("```")
    
    print("</details>")

def main():
    token = None
    if os.environ.get("DEVELOCITY_ACCESS_TOKEN"):
        token = os.environ.get("DEVELOCITY_ACCESS_TOKEN")
    else:
        print("No auth token was specified. You must set DEVELOCITY_ACCESS_TOKEN to your personal access token.")
        exit(1)

    # Configuration
    BASE_URL = "https://ge.apache.org"
    PROJECT = "kafka"
    QUARANTINE_THRESHOLD_DAYS = 7
    MIN_FAILURE_RATE = 0.1
    RECENT_FAILURE_THRESHOLD = 0.5
    SUCCESS_THRESHOLD = 0.7  # For cleared tests
    MIN_FLAKY_RATE = 0.2    # For flaky regressions

    analyzer = TestAnalyzer(BASE_URL, token)
    
    try:
        # Get test results
        quarantined_results = analyzer.get_test_results(
            PROJECT, 
            threshold_days=QUARANTINE_THRESHOLD_DAYS,
            test_type="quarantinedTest"
        )
        
        regular_results = analyzer.get_test_results(
            PROJECT,
            threshold_days=7,  # Last 7 days for regular tests
            test_type="test"
        )
        
        # Generate reports
        problematic_tests = analyzer.get_problematic_quarantined_tests(
            quarantined_results, 
            QUARANTINE_THRESHOLD_DAYS,
            MIN_FAILURE_RATE,
            RECENT_FAILURE_THRESHOLD
        )
        
        flaky_regressions = analyzer.get_flaky_test_regressions(
            PROJECT,
            regular_results,
            recent_days=7,
            min_flaky_rate=MIN_FLAKY_RATE
        )
        
        cleared_tests = analyzer.get_cleared_tests(
            PROJECT,
            quarantined_results,
            success_threshold=SUCCESS_THRESHOLD
        )
        
        # Print report header
        print(f"\n# Flaky Test Report for {datetime.now(pytz.UTC).strftime('%Y-%m-%d')}")
        print(f"This report was run on {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # Print each section
        print_most_problematic_tests(problematic_tests, QUARANTINE_THRESHOLD_DAYS)
        print_flaky_regressions(flaky_regressions, QUARANTINE_THRESHOLD_DAYS)
        print_cleared_tests(cleared_tests, QUARANTINE_THRESHOLD_DAYS)

    except Exception as e:
        logger.exception("Error occurred during report generation")
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("flaky_test_report.log")
        ]
    )
    main()
