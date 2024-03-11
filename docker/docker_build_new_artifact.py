from datetime import date
import argparse
from distutils.dir_util import copy_tree
from common import execute, jvm_image
import os

def remove_args_and_hardcode_values(file_path, kafka_url):
    with open(file_path, 'r') as file:
        filedata = file.read()
    filedata = filedata.replace("ARG kafka_url", f"ENV kafka_url {kafka_url}")
    filedata = filedata.replace("ARG build_date", f"ENV build_date {str(date.today())}")
    with open(file_path, 'w') as file:
        file.write(filedata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-type", "-type", choices=["jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-url", "-u", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    parser.add_argument("--kafka_release_name", "-r", dest="kafka_release_name", help="Kafka release name in the format of major_version.minor_version.patch_version-rc only. Do not add rc number.")
    args = parser.parse_args()


    # # temp_dir_path = tempfile.mkdtemp()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    new_dir = os.path.join(current_dir, f'docker_official_images',  args.kafka_release_name)
    os.makedirs( new_dir, exist_ok=True)
    copy_tree(f"{current_dir}/jvm", f"{new_dir}/jvm")
    copy_tree(f"{current_dir}/resources", f"{new_dir}/jvm/resources")
    # print(f"Curr dir { os.path.dirname(os.path.realpath(__file__))}")
    # print(f"New dir {os.path.join(os.path.dirname(os.path.realpath(__file__)), args.kafka_release_name)}")
    # print(f"Copying from {os.path.join(os.path.dirname(os.path.realpath(__file__)), 'jvm')} to {os.path.join(os.path.dirname(os.path.realpath(__file__)), args.kafka_release_name)}/jvm")
    # print(f"Copyinh from {os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')} to {os.path.join(os.path.dirname(os.path.realpath(__file__)), args.kafka_release_name)}/jvm/resources")
    remove_args_and_hardcode_values(f"{new_dir}/jvm/Dockerfile", args.kafka_url)
    # command = command.replace("$DOCKER_FILE", f"{new_dir}/jvm/Dockerfile")
    # command = command.replace("$DOCKER_DIR", f"{new_dir}/jvm")
    # try:
    #     execute(command.split())
    # except:
    #     raise SystemError("Docker Image Build failed")
    # finally:
    #     shutil.rmtree(temp_dir_path)


