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
Usage: python notes.py <version> > RELEASE_NOTES.html

Generates release notes for a release in HTML format containing
introductory information about the release with links to the
Kafka docs and the list of issues resolved in the release.

The script will fail if there are any unresolved issues still
marked with the target release. This script should be run after either
resolving all issues or moving outstanding issues to a later release.
"""

from jira import JIRA
import itertools, sys


JIRA_BASE_URL = 'https://issues.apache.org/jira'
MAX_RESULTS = 100 # This is constrained for cloud instances so we need to fix this value


def query(query, **kwargs):
    """
    Fetch all issues matching the JQL query from JIRA and expand paginated results.
    Any additional keyword arguments are forwarded to jira.search_issues.
    """
    results = []
    start_at = 0
    new_results = None
    jira = JIRA(JIRA_BASE_URL)
    while new_results is None or len(new_results) == MAX_RESULTS:
        new_results = jira.search_issues(query, startAt=start_at, maxResults=MAX_RESULTS, **kwargs)
        results += new_results
        start_at += len(new_results)
    return results


def filter_unresolved(issues):
    """
    Some resolutions, including a lack of resolution, indicate that
    the bug hasn't actually been addressed and we shouldn't even
    be able to create a release until they are fixed
    """
    UNRESOLVED_RESOLUTIONS = [None,
                              "Unresolved",
                              "Duplicate",
                              "Invalid",
                              "Not A Problem",
                              "Not A Bug",
                              "Won't Fix",
                              "Incomplete",
                              "Cannot Reproduce",
                              "Later",
                              "Works for Me",
                              "Workaround",
                              "Information Provided"
                              ]
    return [issue for issue in issues if issue.fields.resolution in UNRESOLVED_RESOLUTIONS or issue.fields.resolution.name in UNRESOLVED_RESOLUTIONS]


def issue_link(issue):
    """
    Generates a link to the specified JIRA issue.
    """
    return f"{JIRA_BASE_URL}/browse/{issue.key}"


def render(version, issues):
    """
    Renders the release notes HTML with the given version and issues.
    """
    base_url = "https://kafka.apache.org/"
    docs_path = "documentation.html"
    minor_version_dotless = "".join(version.split(".")[:2]) # i.e., 10 if version == 1.0.1
    def issue_type_key(issue):
        if issue.fields.issuetype.name == 'New Feature':
            return -2
        if issue.fields.issuetype.name == 'Improvement':
            return -1
        return int(issue.fields.issuetype.id)
    by_group = [(k,sorted(g, key=lambda issue: issue.id)) for k,g in itertools.groupby(sorted(issues, key=issue_type_key), lambda issue: issue.fields.issuetype.name)]
    parts = [f"""
<h1>Release Notes - Kafka - Version {version}</h1>
<p>
    Below is a summary of the JIRA issues addressed in the {version}
    release of Kafka. For full documentation of the release, a guide
    to get started, and information about the project, see the
    <a href="{base_url}">Kafka project site</a>.
</p>
<p>
    <b>Note about upgrades:</b> Please carefully review the
    <a href="{base_url}{minor_version_dotless}/{docs_path}#upgrade">
    upgrade documentation</a> for this release thoroughly before upgrading
    your cluster. The upgrade notes discuss any critical information about
    incompatibilities and breaking changes, performance changes, and any
    other changes that might impact your production deployment of Kafka.
</p>
<p>
    The documentation for the most recent release can be found at
    <a href="{base_url}{docs_path}">{base_url}{docs_path}</a>.
</p>
"""]
    for itype, issues in by_group:
        parts.append(f"<h2>{itype}</h2>")
        parts.append("</ul>")
        for issue in issues:
            link = issue_link(issue)
            key = issue.key
            summary = issue.fields.summary
            parts.append(f"<li>[<a href=\"{link}\">{key}</a>] - {summary}</li>")
        parts.append("</ul>")
    return "\n".join(parts)


def issue_str(issue):
    """
    Provides a human readable string representation for the given issue.
    """
    key = "%15s" % issue.key
    resolution = "%15s" % issue.fields.resolution
    link = issue_link(issue)
    return f"{key} {resolution} {link}"

def generate(version):
    """
    Generates the release notes in HTML format for given version.
    Raises an error if there are unresolved issues or no issues
    at all for the specified version.
    """
    issues = query(f"project=KAFKA and fixVersion={version}")
    if not issues:
        raise Exception(f"Didn't find any issues for version {version}")
    unresolved_issues = filter_unresolved(issues)
    if unresolved_issues:
        issue_list = "\n".join([issue_str(issue) for issue in unresolved_issues])
        raise Exception(f"""
Release {version} is not complete since there are unresolved or improperly
resolved issues tagged {version} as the fix version:

{issue_list}

Note that for some resolutions, you should simply remove the fix version
as they have not been truly fixed in this release.
        """)
    return render(version, issues)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python notes.py <version>", file=sys.stderr)
        sys.exit(1)

    version = sys.argv[1]
    try:
       print(generate(version))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
