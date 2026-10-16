#
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
#

"""
Interactive test script for the Docker workflow trigger flow.

This invokes the actual trigger_docker_workflows() function from release.py
without needing GPG, SVN, Maven, or committer access. The function is
extracted from release.py without executing its top-level interactive code.

Usage:
  # Dry-run (no API calls, no token needed — recommended for first test):
  GITHUB_DRY_RUN=true python test_docker_trigger_interactive.py

  # Against your fork (real API calls, needs a GitHub token):
  GITHUB_REPO=yourusername/kafka python test_docker_trigger_interactive.py

  # Combine both:
  GITHUB_DRY_RUN=true GITHUB_REPO=yourusername/kafka python test_docker_trigger_interactive.py
"""

import os
import sys

# Ensure release/ is on the path
sys.path.insert(0, os.path.dirname(__file__))

from runtime import confirm, confirm_or_fail, prompt
import gh_actions
import preferences
import templates


def _load_trigger_docker_workflows():
    """
    Extract trigger_docker_workflows from release.py without executing the
    module's top-level interactive code. We parse the source and compile just
    the function definition, then bind it to real (not mocked) dependencies.
    """
    release_path = os.path.join(os.path.dirname(__file__), "release.py")
    with open(release_path) as f:
        source = f.read()

    lines = source.split('\n')
    func_lines = []
    capturing = False
    for line in lines:
        if line.startswith('def trigger_docker_workflows('):
            capturing = True
        elif capturing and line and not line[0].isspace() and not line.startswith('#'):
            break
        if capturing:
            func_lines.append(line)

    func_source = '\n'.join(func_lines)

    ns = {
        'gh_actions': gh_actions,
        'confirm': confirm,
        'confirm_or_fail': confirm_or_fail,
        'preferences': preferences,
        'templates': templates,
        'prompt': prompt,
    }
    exec(compile(func_source, release_path, 'exec'), ns)
    return ns['trigger_docker_workflows']


if __name__ == "__main__":
    trigger_docker_workflows = _load_trigger_docker_workflows()

    print("=" * 70)
    print("  Docker Workflow Trigger - Interactive Test")
    print("=" * 70)
    print(f"\n  Target repo  : {gh_actions.GITHUB_REPO}")
    print(f"  Dry-run mode : {gh_actions.DRY_RUN}")
    print()

    release_version = prompt("Enter release version (e.g. 4.3.0): ")
    rc = prompt("Enter RC number (e.g. 0): ")
    rc_tag = f"{release_version}-rc{rc}"
    dev_branch = '.'.join(release_version.split('.')[:2])

    print(f"\n  Release version : {release_version}")
    print(f"  RC tag          : {rc_tag}")
    print(f"  Dev branch      : {dev_branch}")

    trigger_docker_workflows(rc_tag, release_version, dev_branch)

    print("\nDone.")
