import os
from bs4 import BeautifulSoup
from github import Github
import yaml

### GET THE NAMES OF THE KAFKA COMMITTERS FROM THE apache/kafka-site REPO ###
github_token = os.environ.get('GITHUB_TOKEN')
g = Github(github_token)
repo = g.get_repo("apache/kafka-site")
contents = repo.get_contents("committers.html")
content = contents.decoded_content
soup = BeautifulSoup(content, "html.parser")
committer_logins = [login.text for login in soup.find_all('div', class_='github_login')]

### GET THE CONTRIBUTORS OF THE apache/kafka REPO ###
n = 10
repo = g.get_repo("apache/kafka")
contributors = repo.get_contributors()
collaborators = []
for contributor in contributors:
    if contributor.login not in committer_logins:
        collaborators += [contributor.login]
refreshed_collaborators = collaborators[:n]

### UPDATE asf.yaml ###
file_path = ".asf.yaml"
file = repo.get_contents(file_path)
yaml_content = yaml.safe_load(file.decoded_content)

# Update 'github_whitelist' list
github_whitelist = refreshed_collaborators[:10]  # New users to be added
yaml_content["jenkins"]["github_whitelist"] = github_whitelist

# Update 'collaborators' list
collaborators = refreshed_collaborators[:10]  # New collaborators to be added
yaml_content["github"]["collaborators"] = collaborators

# Convert the updated content back to YAML
updated_yaml = yaml.safe_dump(yaml_content)

# Commit and push the changes
commit_message = "Update .asf.yaml file with refreshed github_whitelist, and collaborators"
repo.update_file(file_path, commit_message, updated_yaml, file.sha)

