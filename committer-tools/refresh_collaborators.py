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
committers, and updating a local configuration file (.asf.yaml) to include these
new contributors.
"""

import io
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from github import Github
from github.Commit import Commit
from github.ContentFile import ContentFile
from github.PaginatedList import PaginatedList
from github.Repository import Repository
from ruamel.yaml import YAML

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
REPO_KAFKA_SITE: str = "apache/kafka-site"
REPO_KAFKA: str = "apache/kafka"
ASF_YAML_PATH: str = "../.asf.yaml"
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


def update_local_yaml_content(yaml_file_path: str, collaborators: List[str]) -> None:
    """
    Update the local .asf.yaml file with refreshed GitHub whitelist and
    collaborators.
    """
    logging.info(
        f"Updating {yaml_file_path} with {len(collaborators)} new collaborators"
    )

    collaborators.sort(key=str.casefold)

    with open(yaml_file_path, "r", encoding="utf-8") as file:
        yaml: YAML = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml_content: dict = yaml.load(file)

    yaml_content["github"]["collaborators"] = collaborators

    with open(yaml_file_path, "w", encoding="utf-8") as file:
        yaml.dump(yaml_content, file)

    logging.info(f"Local file {yaml_file_path} updated successfully")


def main() -> None:
    github_client: Github = get_github_client()

    kafka_site_repo: Repository = github_client.get_repo(REPO_KAFKA_SITE)
    committers: List[str] = get_committers_list(kafka_site_repo)

    kafka_repo: Repository = github_client.get_repo(REPO_KAFKA)
    top_contributors: List[str] = get_top_contributors(kafka_repo, committers)

    update_local_yaml_content(ASF_YAML_PATH, top_contributors)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error: {e}")
