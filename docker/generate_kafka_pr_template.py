import os
import subprocess
import sys
from pathlib import Path


def get_current_branch():
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode('utf-8')


# Returns the hash of the most recent commit that modified any of the specified files.
def file_commit(*files):
    return subprocess.check_output(["git", "log", "-1", "--format=format:%H", "HEAD", "--"] + list(files)).strip().decode('utf-8')


# Returns the latest commit hash for all files in a given directory.
def dir_commit(directory):
    try:
        os.chdir(directory)
        docker_required_scripts = [str(path) for path in Path('.').rglob('*') if path.is_file()]
        files_to_check = ["Dockerfile"] + docker_required_scripts
        return file_commit(*files_to_check)
    finally:
        os.chdir('..')


def main():
    if get_current_branch() != "trunk":
        print("This script can only be run from the trunk branch.")
        sys.exit(1)

    self = os.path.basename(__file__)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    docker_official_images_dir = Path(os.path.join(script_dir, "docker_official_images"))

    os.chdir(docker_official_images_dir)

    highest_version = ""

    header = f"""# This file is generated via https://github.com/apache/kafka/blob/{file_commit(os.path.join(script_dir, self))}/docker/generate_kafka_pr_template.py

Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)
GitRepo: https://github.com/apache/kafka.git
"""
    print(header)

    dirs = sorted([d for d in docker_official_images_dir.iterdir() if d.is_dir()], reverse=True)
    highest_version = max(dirs).name if dirs else ""
    versions = sorted([d for d in docker_official_images_dir.iterdir() if d.is_dir()])

    for dir in versions:
        version = dir.name
        tags = version
        if version == highest_version:
            tags += ", latest"

        commit = dir_commit(os.path.join(dir,"jvm"))

        info = f"""
Tags: {tags}
Architectures: amd64,arm64v8
GitCommit: {commit}
Directory: ./docker/docker_official_images/{version}/jvm
"""
        print(info.strip(), '\n')


if __name__ == "__main__":
    main()
