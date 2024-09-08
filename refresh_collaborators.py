# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This script automates the process of fetching contributor data from GitHub
repositories, filtering top contributors who are not part of the existing
committers, and updating a configuration file (.asf.yaml) in the repository to
include these new contributors.
"""

import io
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from github import Github, GithubException
from github.ContentFile import ContentFile
from github.Repository import Repository
from github.PaginatedList import PaginatedList
from github.Commit import Commit
from ruamel.yaml import YAML

logging.basicConfig(
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
    level=logging.INFO,
)

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
REPO_KAFKA_SITE: str = "apache/kafka-site"
REPO_KAFKA: str = "apache/kafka"
ASF_YAML_PATH: str = ".asf.yaml"
BRANCH_NAME: str = "update-asf-yaml-github-whitelist-and-collaborators"
COMMIT_MESSAGE: str = (
    "MINOR: Update .asf.yaml file with refreshed github_whitelist and collaborators"
)
PR_TITLE: str = COMMIT_MESSAGE
PR_BODY: str = (
    "This pull request updates the github_whitelist and collaborators lists "
    "in .asf.yaml."
)
TOP_N_CONTRIBUTORS: int = 10


def get_github_client() -> Github:
    """
    Initialize GitHub client with token.
    """
    if not GITHUB_TOKEN:
        logging.error("GITHUB_TOKEN is not set in the environment")
        raise ValueError("GITHUB_TOKEN is not set in the environment")

    logging.info("Successfully initialized GitHub client")
    return Github(GITHUB_TOKEN)


def get_committers_list(repo: Repository) -> List[str]:
    """
    Fetch the committers from the given repository.
    """
    logging.info(f"Fetching committers from the repository {REPO_KAFKA_SITE}")
    committers_file: ContentFile = repo.get_contents("committers.html")
    content: bytes = committers_file.decoded_content
    soup: BeautifulSoup = BeautifulSoup(content, "html.parser")

    committers = [login.text for login in soup.find_all("div", class_="github_login")]
    logging.info(f"Found {len(committers)} committers")
    return committers


def get_top_contributors(repo: Repository, committers: List[str]) -> List[str]:
    """
    Get top contributors for the given repository excluding committers.
    """
    logging.info(f"Fetching contributors from the repository {REPO_KAFKA}")
    one_year_ago: datetime = datetime.now() - timedelta(days=365)
    contributors: Dict[str, int] = {}

    last_year_commits: PaginatedList[Commit] = repo.get_commits(since=one_year_ago)
    for contributor in repo.get_contributors():
        if contributor.login not in committers:
            contributions: int = 0
            for commit in last_year_commits:
                if commit.author == contributor:
                    contributions += 1
            contributors[contributor.login] = contributions

    sorted_contributors: List[Tuple[str, int]] = sorted(
        contributors.items(), key=lambda x: x[1], reverse=True
    )

    top_contributors = [login for login, _ in sorted_contributors][:TOP_N_CONTRIBUTORS]
    logging.info(
        f"Found {len(top_contributors)} top contributors who are not committers"
    )
    return top_contributors


def update_yaml_content(repo: Repository, collaborators: List[str]) -> Tuple[str, str]:
    """
    Update the .asf.yaml file with refreshed GitHub whitelist and collaborators.
    """
    logging.info(
        f"Updating {ASF_YAML_PATH} with {len(collaborators)} new collaborators"
    )
    yaml: YAML = YAML()
    file = repo.get_contents(ASF_YAML_PATH)
    yaml_content: dict = yaml.load(file.decoded_content)

    yaml_content["jenkins"]["github_whitelist"] = collaborators
    yaml_content["github"]["collaborators"] = collaborators.copy()

    updated_yaml: io.StringIO = io.StringIO()
    yaml.dump(yaml_content, updated_yaml)

    logging.info(f"File {ASF_YAML_PATH} updated successfully")
    return updated_yaml.getvalue(), file.sha


def create_or_update_branch_and_pr(
    repo: Repository, file_sha: str, updated_yaml_content: str
) -> None:
    """
    Create or update a GitHub branch and open a pull request.
    """
    try:
        repo.get_branch(branch=BRANCH_NAME)
        logging.info(f"Branch {BRANCH_NAME} exists, updating it")
    except GithubException:
        default_branch = repo.get_branch(repo.default_branch)
        repo.create_git_ref(f"refs/heads/{BRANCH_NAME}", default_branch.commit.sha)
        logging.info(f"Created new branch {BRANCH_NAME}")

    repo.update_file(
        ASF_YAML_PATH,
        COMMIT_MESSAGE,
        updated_yaml_content,
        file_sha,
        branch=BRANCH_NAME,
    )
    logging.info(f"Committed changes to {BRANCH_NAME}")

    repo.create_pull(
        title=PR_TITLE, body=PR_BODY, base=repo.default_branch, head=BRANCH_NAME
    )
    logging.info(f"Pull request created successfully with title: {PR_TITLE}")


def main() -> None:
    github_client: Github = get_github_client()

    kafka_site_repo: Repository = github_client.get_repo(REPO_KAFKA_SITE)
    committers: List[str] = get_committers_list(kafka_site_repo)

    kafka_repo: Repository = github_client.get_repo(REPO_KAFKA)
    top_contributors: List[str] = get_top_contributors(kafka_repo, committers)

    updated_yaml_content, file_sha = update_yaml_content(kafka_repo, top_contributors)
    create_or_update_branch_and_pr(kafka_repo, file_sha, updated_yaml_content)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
