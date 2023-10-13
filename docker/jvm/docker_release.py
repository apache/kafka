import subprocess
from datetime import date

def main():
    image = "apache/kafka:3.5.1"
    # Refactor this and extract to constant
    result = subprocess.run(["docker", "build", "-t", image, "--build-arg", "kafka_url=https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz",
                            "--build-arg", f'build_date={date.today()}', "--build-arg", "kafka_version=2.13-3.5.1", "."])
    if result.stderr:
        print(result.stdout)
        return
    subprocess.run(["python", "docker_sanity_test.py", image])

if __name__ == '__main__':
    main()
