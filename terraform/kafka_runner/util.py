import boto3
import logging
import os
import time
import subprocess
from io import StringIO
import requests

from paramiko import SSHClient
from ducktape.cluster.remoteaccount import IgnoreMissingHostKeyPolicy

HASH_ALGORITHM = "sha224"
BASE_MUCKRAKE_DIR = os.path.join(os.path.dirname(__file__), "..")
ABS_MUCKRAKE_DIR = os.path.abspath(BASE_MUCKRAKE_DIR)


logging.getLogger("paramiko").setLevel(logging.WARNING)

AWS_REGION = "us-west-1"
AWS_ACCOUNT_ID = boto3.client('sts', region_name=AWS_REGION).get_caller_identity().get('Account')
AWS_IAM = boto3.client('sts', region_name=AWS_REGION).get_caller_identity().get('Arn').split("/")[1]
AMI_NAME_MAX_LENGTH = 128

AMI= "ami-29ebb519"
INSTANCE_TYPE= "c4.xlarge"

# Various ways to install the confluent platform components
SOURCE_INSTALL = "source"
TARBALL_INSTALL = "tarball"
DISTRO_INSTALL = "distro"
VERSION_TARBALL_INSTALL = "version_tarball"
# Packer constants
AWS_PACKER_JSON = 'vagrant/aws-packer.json'
WORKER_AMI_JSON = 'vagrant/worker-ami.json'
ARM_AWS_PACKER_JSON = 'vagrant/arm-aws-packer.json'
ARM_WORKER_AMI_JSON = 'vagrant/arm-worker-ami.json'

def ssh(host, command, port=22, username='terraform', password=None, key_file=f'{ABS_MUCKRAKE_DIR}/muckrake.pem'):
    client = SSHClient()
    client.set_missing_host_key_policy(IgnoreMissingHostKeyPolicy())

    client.connect(
        hostname=host,
        port=port,
        username=username,
        password=password,
        key_filename=key_file,
        look_for_keys=False)
    _stdin, stdout, stderr = client.exec_command(command)
    code = stdout.channel.recv_exit_status()
    stdout = stdout.read()
    stderr = stderr.read()
    client.close()
    return code, stdout, stderr

def run(cmd, venv=False, venv_dir="venv", print_output=False, allow_fail=True, return_stdout=False, cwd=None):
    if venv:
        # On Ubuntu, it can be necessary to unset PYTHONPATH in order to avoid picking up dist-utils
        cmd = ("unset PYTHONPATH; . %s/bin/activate; " % venv_dir) + cmd
    logging.info("Running command: %s" % cmd)
    proc = subprocess.Popen(
        cmd, shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        executable='/bin/bash', cwd=cwd
    )
    lines = StringIO()
    for line in iter(proc.stdout.readline, b''):
        line = line.decode()
        if print_output:
            logging.info(line.strip())
        lines.write(line)

    output, err = proc.communicate()
    logging.info(output)
    returncode = proc.returncode
    if returncode != 0:
        if allow_fail:
            logging.warning("Command failed with code %s: %s: %s" % (returncode, cmd, err))
        else:
            raise RuntimeError("Command failed with code %s: %s: %s" % (returncode, cmd, err))

    logging.info("finished command " + cmd)
    return lines.getvalue() if return_stdout else returncode


def _is_not_packaging_build_url(resource_url):
    return 'jenkins-confluent' not in resource_url

def _is_not_deb_or_rpm_url(resource_url):
    return '/rpm/' not in resource_url and '/deb/' not in resource_url


def get_valid_resource_url(resource_url):
    # resource url should be either deb or rpm
    # Don't add any extra content to stdout. because stdout content will directly be assigned to resource_url.
    if _is_not_packaging_build_url(resource_url) or _is_not_deb_or_rpm_url(resource_url):
        print(resource_url)
        return
    # resource url example: https://jenkins-confluent-packages.s3.us-west-2.amazonaws.com/7.6.x/101/deb/7.6
    build_url = resource_url.split("/")
    build_number = int(build_url[-3])
    # will pick the resource url with the highest success build number
    for num in range(build_number, 0, -1):
        build_url[-3] = str(num)
        cur_resource_url = '/'.join(build_url)
        if requests.get(cur_resource_url + '/archive.key').ok:
            print(cur_resource_url)
            return
    raise ValueError(f"Resource URL is not available for branch {build_url[-4]}")