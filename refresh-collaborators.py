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

import os
from bs4 import BeautifulSoup
from github import Github
import yaml
from datetime import datetime, timedelta

### GET THE NAMES OF THE KAFKA COMMITTERS FROM THE apache/kafka-site REPO ###
github_token = os.environ.get('GITHUB_TOKEN')
g = Github(github_token)
repo = g.get_repo("apache/kafka-site")
contents = repo.get_contents("committers.html")
content = contents.decoded_content
soup = BeautifulSoup(content, "html.parser")
committer_logins = [login.text for login in soup.find_all('div', class_='github_login')]

### GET THE CONTRIBUTORS AND THEIR COMMIT VOLUME OVER THE LAST YEAR TO THE apache/kafka REPO ###
n = 10
contributors_login_to_commit_volume = {}
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
for commit in repo.get_commits(since=start_date, until=end_date):
    login = commit.author.login
    contributors_login_to_commit_volume[login] = contributors_login_to_commit_volume.get(login, 0) + 1
contributors_login_to_commit_volume = dict(sorted(contributors_login_to_commit_volume.items(), key=lambda x: x[1], reverse=True))
collaborators = []
for contributor_login in contributors_login_to_commit_volume:
    if contributor_login not in committer_logins:
        collaborators += [contributor_login]
refreshed_collaborators = collaborators[:n]

### UPDATE asf.yaml ###
file_path = ".asf.yaml"
file = repo.get_contents(file_path)
yaml_content = yaml.safe_load(file.decoded_content)

# Update 'github_whitelist' list
github_whitelist = refreshed_collaborators  # New users to be added
yaml_content["jenkins"]["github_whitelist"] = github_whitelist

# Update 'collaborators' list
collaborators = refreshed_collaborators  # New collaborators to be added
yaml_content["github"]["collaborators"] = collaborators

# Convert the updated content back to YAML
updated_yaml = yaml.safe_dump(yaml_content)

# Commit and push the changes
commit_message = "MINOR: Update .asf.yaml file with refreshed github_whitelist, and collaborators"
repo.update_file(file_path, commit_message, updated_yaml, file.sha)

