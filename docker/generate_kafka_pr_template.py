import os
import subprocess
from git import Repo
from pathlib import Path

repo = Repo('.')
current_branch = repo.active_branch.name
if current_branch != "trunk":
    print("This script can only be run from the trunk branch.")
    exit(1)

def file_commit(*paths):
    """Function to get the most recent commit which modified any of given paths."""
    commits = repo.iter_commits(paths=paths, max_count=1)
    return next(commits, None)

def dir_commit(dir_path):
    """Function to get the most recent commit which modified dir_path/Dockerfile or any file COPY'd from dir_path/Dockerfile."""
    dockerfile_path = dir_path / "Dockerfile"
    if not dockerfile_path.exists():
        return None
    with dockerfile_path.open() as dockerfile:
        copy_files = [
            line.split()[1]
            for line in dockerfile
            if line.upper().startswith("COPY ")
        ]
    paths = [str(dockerfile_path)] + copy_files
    return file_commit(*paths)

def main():
    script_path = Path(__file__).resolve()
    script_commit = file_commit(script_path)
    script_commit_hash = script_commit.hexsha if script_commit else "N/A"

    os.chdir("docker_official_images")
    versions = sorted([d for d in Path('.').iterdir() if d.is_dir()], reverse=True)
    highest_version = versions[0].name
    highest_major_minor = '.'.join(highest_version.split('.')[:2])

    print(f"# This file is generated via https://github.com/apache/kafka/blob/{script_commit_hash}/docker/generate_kafka_pr_template.sh")
    print("Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)")
    print("GitRepo: https://github.com/apache/kafka.git")

    for version_dir in versions:
        version = version_dir.name
        major_minor_version = '.'.join(version.split('.')[:2])
        latest_in_series = sorted(version_dir.parent.glob(f"{major_minor_version}.*"), reverse=True)[0].name

        tags = version
        if version == latest_in_series and major_minor_version == highest_major_minor:
            tags += ", latest"

        commit = dir_commit(version_dir / "jvm")
        commit_hash = commit.hexsha if commit else "N/A"

        print(f"\nTags: {tags}")
        print("Architectures: amd64,arm64v8")
        print(f"GitCommit: {commit_hash}")
        print(f"Directory: ./docker/docker_official_images/{version}/jvm")

if __name__ == "__main__":
    main()
