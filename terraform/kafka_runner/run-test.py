# Copyright 2024 Confluent Inc.
import json
import logging
import os
import shutil
import sys
import subprocess

import time
from datetime import datetime, timedelta, timezone
from functools import partial
from traceback import format_exc
from jinja2 import Environment, FileSystemLoader
from ducktape.utils.util import wait_until
from paramiko.ssh_exception import NoValidConnectionsError

from terraform.kafka_runner.util import run, SOURCE_INSTALL,ssh
from terraform.kafka_runner.package_ami import package_worker_ami
from terraform.kafka_runner.util import INSTANCE_TYPE, ABS_KAFKA_DIR, JOB_ID, AWS_REGION, AWS_ACCOUNT_ID, AMI, IPV4_SUBNET_ID,IPV6_SUBNET_ID, IS_IPV6_RUN
from terraform.kafka_runner.util import setup_virtualenv, parse_args, parse_bool
from ssh_checkers.aws_checker import aws_ssh_checker

class KafkaRunner:
    kafka_dir = ABS_KAFKA_DIR
    cluster_file_name = f"{kafka_dir}/tf-cluster.json"
    tf_variables_file = f"{kafka_dir}/tf-vars.tfvars.json"

    def __init__(self, args, venv_dir):
        self.args = args
        self._terraform_outputs = None
        self.venv_dir = venv_dir
        self.public_key = os.environ['MUCKRAKE_SECRET']

    def _run_creds(self, cmd, *args, **kwargs):
        """ Assuming semaphore-access role to facilitate semaphore node to maintain the ec2 lifecycle and perform necessary action """
        return run(f". assume-iam-role arn:aws:iam::419470726136:role/semaphore-access> /dev/null; cd {self.kafka_dir}; {cmd}", *args, **kwargs)

    def terraform_outputs(self):
        """Returning the output of terraform command"""
        if not self._terraform_outputs:
            raw_json = self._run_creds(f"terraform output -json", print_output=True, allow_fail=False,
                                       return_stdout=True, cwd=self.kafka_dir)
            self._terraform_outputs = json.loads(raw_json)
        return self._terraform_outputs

    def update_hosts(self):
        """Function to update host file of workers nodes"""
        cmd = "sudo bash -c 'echo \""
        terraform_outputs_dict = self.terraform_outputs()
        worker_names = terraform_outputs_dict['worker-names']["value"]
        worker_ips = [ip[0] for ip in terraform_outputs_dict['worker-ipv6s']["value"]] if IS_IPV6_RUN else terraform_outputs_dict['worker-private-ips']["value"]
        worker_instance_id= terraform_outputs_dict['worker-instance-ids']["value"]
        for hostname, ip in zip(worker_names, worker_ips):
            cmd += f"{ip} {hostname} \n"
        cmd += "\" >> /etc/hosts'"
        run_cmd = partial(ssh, command=cmd)

        for host in worker_ips:
            run_cmd(host)
        run(cmd, print_output=True, allow_fail=False)

    def generate_clusterfile(self):
        """Generating cluster file containing information about worker nodes"""
        logging.info("generating cluster file")
        terraform_outputs_dict = self.terraform_outputs()
        worker_names = terraform_outputs_dict['worker-names']["value"]
        worker_ips = [ip[0] for ip in terraform_outputs_dict['worker-ipv6s']["value"]] if IS_IPV6_RUN else terraform_outputs_dict['worker-private-ips']["value"]
        nodes = []
        for hostname, ip in zip(worker_names, worker_ips):
            routable_ip = f"[{ip}]" if IS_IPV6_RUN else ip
            nodes.append({
                "externally_routable_ip": routable_ip,
                "ssh_config": {
                    "host": hostname,
                    "hostname": hostname,
                    "port": 22,
                    "user": "ubuntu",
                    "password": None,
                    "identityfile": os.path.join(os.environ.get('WORKSPACE'),'semaphore-muckrake.pem')
                }
            })
        with open(self.cluster_file_name, 'w') as f:
            json.dump({"nodes": nodes}, f)

    def wait_until_ready(self, timeoutSecond=180, polltime=2):
        """
        Function to wait until worker node is not ready
        """
        terraform_outputs_dict = self.terraform_outputs()
        worker_ips = [ip[0] for ip in terraform_outputs_dict['worker-ipv6s']["value"]] if IS_IPV6_RUN else terraform_outputs_dict['worker-private-ips']["value"]
        worker_instance_id= terraform_outputs_dict['worker-instance-ids']["value"]
        start = time.time()
        # print all instance ids
        for instance_id in worker_instance_id:
            logging.info(f"instance_id: {instance_id}")

        def check_node_boot_finished(host):
            code, _, _ = ssh(host, "[ -f /var/lib/cloud/instance/boot-finished ]")
            return 0==code

        def check_for_ssh(host):
            try:
                ssh(host, "true")
                return True
            except NoValidConnectionsError as e:
                logging.error(f"{e}")
                return False

        def poll_all_nodes():
            # check and see if cloud init is done on all nodes
            unfinished_nodes = [ip for ip in worker_ips if not check_node_boot_finished(ip)]
            # unfinished_nodes = [instance_id for instance_id in worker_instance_id if not check_node_boot_finished(instance_id)]
            result = len(unfinished_nodes) == 0
            if not result:
                time_diff = time.time() - start
                logging.warning(f"{time_diff}: still waiting for {unfinished_nodes}")
            return result
        wait_until(lambda: all(check_for_ssh(ip) for ip in worker_ips),
                   timeoutSecond, polltime, err_msg="ssh didn't become available")
        self.update_hosts()
        logging.warning("updated hosts file")
        wait_until(poll_all_nodes, 15 * 60, 2, err_msg="didn't finish cloudinit")
        logging.info("cloudinit finished on all nodes")

    def tags_to_aws_format(tags):
        """
        :return: key value pairs of tags for aws resources
        """
        kv_format = [f"Key={k},Value={v}" for k,v in tags.items()]
        return f"{' '.join(kv_format)}"

    def generate_tf_file(self):
        """
        Generate terraform file dynamically for resource creation
        :return:
        """
        logging.info("creating terraform file")
        env = Environment(loader=FileSystemLoader(f'{self.kafka_dir}'))
        template = env.get_template('main.tf')
        # this spot instance expiration time.  This is a failsafe, as terraform
        # should cancel the request on a terraform destroy, which occurs on a provission
        # failure
        spot_instance_time = datetime.now(timezone.utc) + timedelta(hours=2)
        spot_instance_time = spot_instance_time.isoformat()
        with open(f'{self.kafka_dir}/main.tf', 'w') as f:
            f.write(template.render())
        logging.info("terraform file created")

    def setup_tf_variables(self, ami):
        """
        param ami: Ami using for spin up worker nodes
        :return:
        Function of set the value of all terraform variables
        """

        num_workers = self.args.num_workers
        vars = {
            "instance_type": self.args.worker_instance_type,
            "worker_ami": ami,
            "num_workers": num_workers,
            "num_workers_spot": 0,
            "deployment": self.args.linux_distro,
            "public_key": self.public_key,
            "spot_price": self.args.spot_price,
            "build_url": self.args.build_url,
            "subnet_id": IPV6_SUBNET_ID if IS_IPV6_RUN else IPV4_SUBNET_ID,
            "ipv6_address_count": 1 if IS_IPV6_RUN else 0,
            "job_id": JOB_ID
        }
        with open(self.tf_variables_file, 'w') as f:
            json.dump(vars, f)

    def provission_terraform(self):
        """
        Resource creation using terraform
        :return:
        """
        logging.info("provisioning tf file")
        self._run_creds(f"terraform --version", print_output=True, allow_fail=False)
        self._run_creds(f"terraform init", print_output=True, allow_fail=False, venv=False, cwd=self.kafka_dir)
        self._run_creds(f"terraform apply -auto-approve -var-file={self.tf_variables_file}", print_output=True, allow_fail=False,
                        venv=False, cwd=self.kafka_dir)

    def destroy_terraform(self, allow_fail=False):
        """
        Destroy all resources that are created by terraform
        """
        self._run_creds(f"terraform init", print_output=True, allow_fail=True, cwd=self.kafka_dir)
        self._run_creds(f"terraform destroy -auto-approve -var-file={self.tf_variables_file}", print_output=True,
                        allow_fail=allow_fail, cwd=self.kafka_dir)

    def install_custom_ducktape_branch(self, ducktape_branch):
        """Override the default ducktape installation with the given branch"""
        for i in range(10):
            try:
                run(f"{self.args.python} -m pip uninstall -y ducktape",
                    print_output=True, venv_dir=self.venv_dir, venv=True, allow_fail=False, cwd=self.muckrake_dir)
            except Exception as e:
                if "Command failed" in str(e):
                    break

        run(f"if [ ! -d ducktape ]; then git clone https://github.com/confluentinc/ducktape.git; fi",
            print_output=True, venv_dir=self.venv_dir, venv=True, allow_fail=False, cwd=self.kafka_dir)
        run(f"git checkout {ducktape_branch} && {self.args.python} setup.py develop",
            print_output=True, venv_dir=self.venv_dir, venv=True, allow_fail=False, cwd=f"{self.kafka_dir}/ducktape")

def main():
    logging.basicConfig(format='[%(levelname)s:%(asctime)s]: %(message)s', level=logging.INFO)
    args, ducktape_args = parse_args()
    kafka_dir = ABS_KAFKA_DIR
    if args.new_globals is not None:
        global_val = json.loads(args.new_globals)
        file_data = open(f'{kafka_dir}/resources/{args.install_type}-globals.json', 'r')
        globals_dict = json.loads(file_data.read())
        file_data.close()
        for key, value in global_val.items():
            globals_dict[key] = value
        with open(f'{kafka_dir}/resources/{args.install_type}-globals.json', "w") as outfile:
            json.dump(globals_dict, outfile, indent = 4)
        logging.info(f"New globals passed - {globals_dict}")
    kafka_dir = ABS_KAFKA_DIR
    venv_dir = os.path.join(os.environ.get('WORKSPACE'), "venv")

    # setup virtualenv directory
    if os.path.exists(venv_dir):
        shutil.rmtree(venv_dir, ignore_errors=True)
        setup_virtualenv(venv_dir, args)

    # reset directory containing source code for CP components
    projects_dir = os.path.join(kafka_dir, "projects")
    if os.path.exists(projects_dir):
        shutil.rmtree(projects_dir)

    test_runner = KafkaRunner(args, venv_dir)
    run(f"{args.python} -m pip install -U -r resources/requirements.txt",
        print_output=True, venv=True, allow_fail=False, cwd=kafka_dir)
    run(f"{args.python} -m pip install -U -r resources/requirement_override.txt",
        print_output=True, venv=True, allow_fail=True, cwd=kafka_dir)
    exit_status = 0

    try:
        # Check that the test path is valid before doing expensive cluster bringup
        # We still do this after the build step since that's how we get kafka, and our ducktape dependency
        test_path = " ".join(args.test_path)
        cmd = f"{args.python} `which ducktape` {test_path} --collect-only"
        run(cmd, venv=True, venv_dir=venv_dir, print_output=True, allow_fail=False)
        if args.collect_only:
            logging.info("--collect-only flag used; exiting without running tests")
            return

        # Skip build if we are re-using an older image
        if args.aws:
            ssh_account = 'ubuntu'
            base_ami = AMI
            if args.existing_ami is None:
                logging.info(f"linux distro input: {args.linux_distro}")
                logging.info(f"base_ami: {base_ami}")
                image_id = package_worker_ami(
                    args.install_type,
                    args.worker_volume_size,
                    source_ami=base_ami,
                    linux_distro=args.linux_distro,
                    instance_type=args.worker_instance_type,
                    ssh_account=ssh_account,
                    instance_name=args.instance_name,
                    jdk_version=args.jdk_version,
                    jdk_arch=args.jdk_arch
                )
            else:
                logging.info(f"using existing ami: {args.existing_ami}")
                image_id = args.existing_ami

        # Take down any existing to bring up cluster from scratch
        logging.info("calling generate_tf_file")
        logging.info(f"ami: {image_id}")
        test_runner.generate_tf_file()
        test_runner.setup_tf_variables(image_id)
        test_runner.destroy_terraform(allow_fail=True)
        cluster_file_name = f"{kafka_dir}/tf-cluster.json"

        if args.aws:
            # re-source vagrant credentials before bringing up cluster
            run(f". assume-iam-role arn:aws:iam::419470726136:role/semaphore-access; cd {kafka_dir};",
                print_output=True, allow_fail=False)
            test_runner.provission_terraform()
            logging.info("calling function to generate cluster file")
            test_runner.generate_clusterfile()
        else:
            run(f"cd {kafka_dir};",
                print_output=True, allow_fail=False)
            test_runner.provission_terraform()
            test_runner.generate_clusterfile()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            with open(f"{kafka_dir}/tf-cluster.json", "r") as f:
                logging.debug(f'starting with cluster: {f.read()}')

        test_runner.wait_until_ready()
    except Exception as ex:
        file_path = "~/infra_failure.log"
        file_expanded = os.path.expanduser(file_path)
        error_txt = "INFRA_FAILURE " + str(ex)
        logging.error(error_txt)
        with open(file_expanded, "w") as f:
            f.write(error_txt)
        logging.info("bringing down terraform cluster")
        test_runner.destroy_terraform()
        sys.exit(1)

    logging.info("Cluster is up and running")

    try:

        source= os.path.join(os.environ.get('WORKSPACE'),'kafka')

        # Run the tests!
        cmd = f"{args.python} `which ducktape` {test_path} " \
              f"--results-root {args.results_root} " \
              f"--default-num-nodes 1 " \
              f"--max-parallel=1000 " \
              f"--cluster ducktape.cluster.json.JsonCluster " \
              f"--cluster-file {cluster_file_name} " \
              f"--compress " \
              f"{' '.join(ducktape_args)}"
        exit_status = run(cmd, venv=True, venv_dir=venv_dir, print_output=True)
        logging.info("Tests " + ("passed" if exit_status == 0 else "failed"))

    except Exception as e:
        logging.warning(e)
        logging.warning(format_exc())
        exit_status = 1
    finally:
        # Cleanup and teardown all workers
        if not args.collect_only and args.cleanup:
            logging.info("bringing down terraform cluster...")
            test_runner.destroy_terraform()
        elif not args.cleanup:
            logging.warning("--cleanup is false, leaving nodes alive")
        sys.exit(exit_status)

if __name__ == "__main__":
    main()