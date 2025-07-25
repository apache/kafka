import boto3
import logging
import os
import time
import subprocess
from io import StringIO
import requests
import argparse
import sys

from paramiko import SSHClient
from ducktape.cluster.remoteaccount import IgnoreMissingHostKeyPolicy
from botocore.exceptions import ClientError

HASH_ALGORITHM = "sha224"
BASE_KAFKA_DIR = os.path.join(os.path.dirname(__file__), "..")
ABS_KAFKA_DIR = os.path.abspath(BASE_KAFKA_DIR)
WORKER_AMI_JSON = '../vagrant/worker-ami.json'
AWS_PACKER_JSON = '../vagrant/aws-packer.json'

# List files in the directory
workspace= os.environ.get('WORKSPACE')
SOURCE_INSTALL = "source"
logging.getLogger("paramiko").setLevel(logging.WARNING)

AWS_REGION = "us-west-2"
AWS_ACCOUNT_ID = boto3.client('sts', region_name=AWS_REGION).get_caller_identity().get('Account')
AWS_IAM = boto3.client('sts', region_name=AWS_REGION).get_caller_identity().get('Arn').split("/")[1]

AMI_NAME_MAX_LENGTH = 128
WORKER_AMI_NAME = 'kafka-{}'.format(  # E.g. BUILD_TAG = semaphore-system-test-kafka-master-452
    os.environ['SEMAPHORE_JOB_ID'] if 'SEMAPHORE_JOB_ID' in os.environ else str(int(time.time())))

AMI= os.environ.get('AMI_ID')
INSTANCE_TYPE= os.environ.get('INSTANCE_TYPE')
IPV4_SUBNET_ID= "subnet-0429253329fde0351"
IPV6_SUBNET_ID= "subnet-00c4999d6841fd454"

VPC_NAME= "system-test-ducktape-infra"
VPC_ID= "vpc-00acc0e3d6688724b"
BRANCH_NAME = os.environ.get('BRANCH_NAME')

ALLOW_ALL_SECURITY_GROUP_ID = "sg-0344366211836c8fe"

KAFKA_BRANCH = os.environ.get('KAFKA_BRANCH')
JOB_ID = os.environ.get('SEMAPHORE_JOB_ID')
IS_IPV6 = os.environ.get('IS_IPV6',"False")
IS_IPV6_RUN = IS_IPV6=="True"
def ssh(host, command, port=22, username='ubuntu', password=None, key_file = os.path.join(os.environ.get('WORKSPACE'),'semaphore-muckrake.pem')):
    """
    :param host: IP address of worker node
    :param command: command that run while doing ssh
    :param username: username of ssh user
    :param key_file: pem file path that require for ssh
    :return: success of ssh or not
    """
    os.chmod(key_file, 0o444)
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
    """Function to use run bash command"""
    if venv:
        # On Ubuntu, it can be necessary to unset PYTHONPATH in order to avoid picking up dist-utils
        cmd = ("unset PYTHONPATH; . %s/bin/activate; " % venv_dir) + cmd
    logging.info(f"Running command: {cmd}")
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

    logging.info(f"finished command {cmd}")
    return lines.getvalue() if return_stdout else returncode

def setup_virtualenv(venv_dir, args):
    """Install virtualenv if necessary, and create VIRTUAL_ENV directory with virtualenv command."""
    if run("which virtualenv") != 0:
        # install virtualenv if necessary
        logging.info("No virtualenv found. Installing...")
        run(f"{args.python} -m pip install virtualenv",
            allow_fail=False)
        logging.info("Installation of virtualenv succeeded")

    if not os.path.exists(venv_dir):
        logging.info("Setting up virtualenv...")
        run(f"virtualenv -p {args.python} {venv_dir}", allow_fail=False)
        run("pip install --upgrade pip setuptools", venv=True, allow_fail=False)

def parse_args():
    """
    This is function is parsing all argument that we are passing while running the file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws", action="store_true", help="use commands for running on AWS")
    parser.add_argument("--image-name", action="store", type=str, default=os.environ.get('IMAGE_NAME'),
                        help="Name of image to use for virtual machines.")
    parser.add_argument("--install-type", action="store", default=SOURCE_INSTALL,
                        help="how Confluent Platform will be installed")
    parser.add_argument("--instance-name", action="store", default="KAFKA_TEST_SEMAPHORE",
                        help="Name of AWS instances")
    parser.add_argument("--worker-instance-type", action="store", default=INSTANCE_TYPE,
                        help="AWS instance type to be used for worker nodes.")
    parser.add_argument("--worker-volume-size", action="store", type=int,
                        default=40, help="Volume size in GB to be used for worker nodes. Min 50GB.")
    parser.add_argument("--vagrantfile", action="store", default="system-test/resources/scripts/system-tests/kafka-system-test/Vagrantfile.local",
                        help="specify location of template for Vagrantfile.local")
    parser.add_argument("--num-workers", action="store", type=int,
                        default=1, help="number of worker nodes to bring up")
    parser.add_argument("--enable-pwd-scan", action="store_true",
                        help="run password scanner with --enable-pwd-scan flag."
                             "Scan for passwords from constants file in logs ")
    parser.add_argument("--notify-slack-with-passwords", action="store_true",
                        help="Notify slack channel with passwords found")
    parser.add_argument("--results-root", action="store", default="./results", help="direct test output here")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='*',
                        default=["tests/kafkatest/tests/core/security_test.py"],
                        help="run tests found underneath this directory. "
                             "This directory is relative to root muckrake directory.")
    parser.add_argument("--collect-only", action="store_true", help="run ducktape with --collect-only flag")
    parser.add_argument("--cleanup", action="store", type=parse_bool, default=True,
                        help="Tear down instances after tests.")
    parser.add_argument("--repeat", action="store", type=int, default=1,
                        help="Use this flag to repeat all discovered tests the given number of times.")
    parser.add_argument("--parallel", action="store_true", help="if true, run tests in parallel")
    parser.add_argument("--linux-distro", action="store", type=str, default="ubuntu",
                        help="The linux distro to install on.")
    parser.add_argument("--sample", action="store", type=int, default=None,
                        help="The size of a random sample of tests to run")
    parser.add_argument("--python", action="store", type=str, default="python", help="The python executable to use")
    parser.add_argument("--build-url", action="store", type=str, default="kafka.test.us",
                        help="The Jenkins Build URL to tag AWS Resources")
    parser.add_argument("--parameters", action="store", type=str, default=None, help="Override test parameter")
    parser.add_argument("--spot-instance", action="store_true", help="run as spot instances")
    parser.add_argument("--spot-price", action="store", type=float, default=0.266, help="maximum price for a spot instance")
    parser.add_argument("--ssh-checker", action="store", nargs='+',
                        default=['ssh_checkers.aws_checker.aws_ssh_checker'],
                        help="full module path of functions to run in the case of an ssh error")
    parser.add_argument("--jdk-version", action="store", type=str, default="8",
                        help="JDK version to install on the nodes."),
    parser.add_argument("--jdk-arch", action="store", type=str, default="x64",
                        help="JDK arch to execute."),
    parser.add_argument("--nightly", action="store_true", default=False, help="Mark this as a nightly run")
    parser.add_argument("--new-globals", action="store", type=str, default=None, help="Additional global params to be passed in ducktape")
    parser.add_argument("--arm-image", action="store_true", help="load the ARM based image of specified distro")
    parser.add_argument("--existing-ami", action="store", type=str, default=None, help="AMI ID to use for the instance, skipping ami creation")
    args, rest = parser.parse_known_args(sys.argv[1:])

    return args, rest

def parse_bool(s):
    return True if s and s.lower() not in ('0', 'f', 'no', 'n', 'false') else False