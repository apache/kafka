import argparse
import json
import logging
import os
import shutil
import sys
import requests


import time
from datetime import datetime, timedelta, timezone
from functools import partial
from traceback import format_exc
from jinja2 import Environment, FileSystemLoader
from ducktape.utils.util import wait_until
from paramiko.ssh_exception import NoValidConnectionsError

from kafka_runner.util import ssh, run, SOURCE_INSTALL
from kafka_runner.util import INSTANCE_TYPE


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

def validate_args(args):
    if args.install_type != SOURCE_INSTALL:
        assert args.resource_url, "If running a non-source install, a resource url is required."
    resource_response = check_resource_url(args.resource_url)
    assert resource_response, "resource url is either invalid or the connection has failed"
    
def check_resource_url(resource_url):
    if resource_url.endswith('.tar.gz'):
        return requests.get(resource_url).ok
    if '/rpm/' in resource_url or '/deb/' in resource_url:
        return requests.get(resource_url + '/archive.key').ok
    return True

def parse_bool(s):
    return True if s and s.lower() not in ('0', 'f', 'no', 'n', 'false') else False        

class kafka_runner:
    cluster_file_name = f"{muckrake_dir}/tf-cluster.json"
    tf_variables_file = f"{muckrake_dir}/tf-vars.tfvars.json"

    def __init__(self, args, venv_dir):
        self.args = args
        self._terraform_outputs = None
        self.venv_dir = venv_dir
        self.public_key = self.get_vault_secret('semaphore-muckrake', 'pub').strip()

    def terraform_outputs(self):
        if not self._terraform_outputs:
            raw_json = self._run_creds(f"terraform output -json", print_output=True, allow_fail=False,
                           return_stdout=True, cwd=self.muckrake_dir)
            self._terraform_outputs = json.loads(raw_json)
        return self._terraform_outputs
     
    def update_hosts(self):
        cmd = "sudo bash -c 'echo \""

        worker_names = self.terraform_outputs['worker-names']["value"]
        worker_ips = self.terraform_outputs['worker-private-ips']["value"]

        for hostname, ip in zip(worker_names, worker_ips):
            cmd += f"{ip} {hostname} \n"
        cmd += "\" >> /etc/hosts'"
        run_cmd = partial(ssh, command=cmd)

        for host in worker_ips:
            run_cmd(host)
        run(cmd, print_output=True, allow_fail=False)

    def generate_clusterfile(self):
        worker_names = self.terraform_outputs['worker-names']["value"]
        worker_ips = self.terraform_outputs['worker-private-ips']["value"]

        nodes = []

        for hostname, ip in zip(worker_names, worker_ips):
            nodes.append({
                "externally_routable_ip": ip,
                "ssh_config": {
                    "host": hostname,
                    "hostname": hostname,
                    "port": 22,
                    "user": "terraform",
                    "password": None,
                    "identityfile": "./muckrake.pem"
                }
            })
        with open(self.cluster_file_name, 'w') as f:
            json.dump({"nodes": nodes}, f)

    def wait_until_ready(self, timeout=120, polltime=2):
        worker_ips = self.terraform_outputs['worker-private-ips']["value"]

        start = time.time()


    def check_node_boot_finished(host):
            # command to check and see if cloud init finished
            code, _, _ = ssh(host, "[ -f /var/lib/cloud/instance/boot-finished ]")
            return 0 == code

    def check_for_ssh(host):
        try:
            ssh(host, "true")
            return True
        except NoValidConnectionsError:
            return False

    def generate_tf_file(self):
        env = Environment(loader=FileSystemLoader(f'{self.muckrake_dir}/templates'))
        template = env.get_template('jenkins.tf')

        # this spot instance expiration time.  This is a failsafe, as terraform
        # should cancel the request on a terraform destroy, which occurs on a provission
        # failure
        spot_instance_time = datetime.now(timezone.utc) + timedelta(hours=2)
        spot_instance_time = spot_instance_time.isoformat()
        tags = {
            "Name": "kafka-worker",
            "Owner": "ce-kafka",
            "role": "ce-kafka",
            "JenkinsBuildUrl": self.args.build_url,
            "cflt_environment": "devel",
            "cflt_partition": "onprem",
            "cflt_managed_by": "iac",
            "cflt_managed_id": "kafka",
            "cflt_service": "kafka"
        }   
