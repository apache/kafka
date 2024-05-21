import os
import subprocess
import sys
from pathlib import Path

def get_current_branch():
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode('utf-8')

def file_commit(*files):
    return subprocess.check_output(["git", "log", "-1", "--format=format:%H", "HEAD", "--"] + list(files)).strip().decode('utf-8')

def dir_commit(directory):
    try:
        os.chdir(directory)
        dockerfile_content = subprocess.check_output(["git", "show", "HEAD:./Dockerfile"]).decode('utf-8')
        copy_files = [line.split()[1] for line in dockerfile_content.splitlines() if line.upper().startswith("COPY")]
        files_to_check = ["Dockerfile"] + copy_files
        return file_commit(*files_to_check)
    finally:
        os.chdir('..')

def main():
    if get_current_branch() != "trunk":
        print("This script can only be run from the trunk branch.")
        sys.exit(1)

    self = os.path.basename(__file__)
    script_dir = Path(os.path.dirname(os.path.abspath(__file__))).resolve()
    docker_official_images_dir = script_dir / "docker_official_images"

    os.chdir(docker_official_images_dir)

    highest_version = ""

    # Output header information
    header = f"""# This file is generated via https://github.com/apache/kafka/blob/{file_commit(str(script_dir / self))}/docker/generate_kafka_pr_template.py

Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)
GitRepo: https://github.com/apache/kafka.git
"""
    print(header)

    dirs = sorted([d for d in docker_official_images_dir.iterdir() if d.is_dir()], reverse=True)
    highest_version = max(dirs).name if dirs else ""
    versions = sorted([d for d in docker_official_images_dir.iterdir() if d.is_dir()])

    for dir in versions:
        version = dir.name
        major_minor_version = '.'.join(version.split('.')[:2])
        highest_major_minor = '.'.join(highest_version.split('.')[:2])

        latest_in_series = sorted(d for d in docker_official_images_dir.glob(f"{major_minor_version}.*") if d.is_dir())[-1]
        latest_in_series_basename = latest_in_series.name

        tags = version
        if version == latest_in_series_basename and major_minor_version == highest_major_minor:
            tags += ", latest"

        commit = dir_commit(dir / "jvm")

        info = f"""
Tags: {tags}
Architectures: amd64,arm64v8
GitCommit: {commit}
Directory: ./docker/docker_official_images/{version}/jvm
"""
        print(info.strip())

if __name__ == "__main__":
    main()
