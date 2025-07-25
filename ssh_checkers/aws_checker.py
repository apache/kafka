import subprocess
import logging
from ducktape.utils.util import wait_until

DEFAULT_AWS_COMMAND_TIMEOUT_SECOND = 360


def aws_ssh_checker(error, remote_account):
    """This function use to check if node is still and running or termination.
    Also semaphore agent able to do ssh
    (this should be just one node if the node is still up, and zero
    if its terminated)
    Args:
        error (Exception): the ssh exception passed from ducktapes remote account
        remote_account (RemoteAccount): ducktapes remote account object experiencing the
        ssh failure
    Raises:
        Exception: when the aws node is missing
    """
    remote_account.logger.log(logging.INFO, 'running aws_checker:')

    cmd = ['aws','ec2','describe-instances','--filters',
           '"Name=private-ip-address,Values={}"'.format(remote_account.externally_routable_ip),
           '--query', '"Reservations[].Instances[].Tags[?Key==\'Name\']|[0][0].Value"',
           '--region', 'us-west-2']
    remote_account.logger.log(logging.INFO, 'running command {}'.format(cmd))
    result = subprocess.Popen(" ".join(cmd),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    wait_until(result.poll, DEFAULT_AWS_COMMAND_TIMEOUT_SECOND)
    output = [line.decode() for line in iter(result.stdout.readline, b'') if line.strip() and line.strip() != b'null']
    if result.returncode != 0:
        remote_account.logger.log(logging.ERROR, 'aws command "{}" failed:'.format(" ".join(cmd)))
        remote_account.logger.log(logging.ERROR, result.stdout.read().decode())
        remote_account.logger.log(logging.ERROR, result.stderr.read().decode())
    if not output:
        raise Exception("AWS host no longer exists {}".format(remote_account.externally_routable_ip))