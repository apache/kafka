# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os.path

from ducktape.services.service import Service
from ducktape.utils import util


class KiboshService(Service):
    """
    Kibosh is a fault-injecting FUSE filesystem.

    Attributes:
        INSTALL_ROOT                    The path of where Kibosh is installed.
        BINARY_NAME                     The Kibosh binary name.
        BINARY_PATH                     The path to the kibosh binary.
    """
    INSTALL_ROOT = "/opt/kibosh/build"
    BINARY_NAME = "kibosh"
    BINARY_PATH = os.path.join(INSTALL_ROOT, BINARY_NAME)

    def __init__(self, context, nodes, target, mirror, persist="/mnt/kibosh"):
        """
        Create a Kibosh service.

        :param context:             The TestContext object.
        :param nodes:               The nodes to put the Kibosh FS on.  Kibosh allocates no
                                    nodes of its own.
        :param target:              The target directory, which Kibosh exports a view of.
        :param mirror:              The mirror directory, where Kibosh injects faults.
        :param persist:             Where the log files and pid files will be created.
        """
        Service.__init__(self, context, num_nodes=0)
        if (len(nodes) == 0):
            raise RuntimeError("You must supply at least one node to run the service on.")
        for node in nodes:
            self.nodes.append(node)

        self.target = target
        self.mirror = mirror
        self.persist = persist

        self.control_path = os.path.join(self.mirror, "kibosh_control")
        self.pidfile_path = os.path.join(self.persist, "pidfile")
        self.stdout_stderr_path = os.path.join(self.persist, "kibosh-stdout-stderr.log")
        self.log_path = os.path.join(self.persist, "kibosh.log")
        self.logs = {
            "kibosh-stdout-stderr.log": {
                "path": self.stdout_stderr_path,
                "collect_default": True},
            "kibosh.log": {
                "path": self.log_path,
                "collect_default": True}
        }

    def free(self):
        """Clear the nodes list."""
        # Because the filesystem runs on nodes which have been allocated by other services, those nodes
        # are not deallocated here.
        self.nodes = []
        Service.free(self)

    def kibosh_running(self, node):
        return 0 == node.account.ssh("test -e '%s'" % self.control_path, allow_fail=True)

    def start_node(self, node):
        node.account.mkdirs(self.persist)
        cmd = "sudo -E "
        cmd += " %s" % KiboshService.BINARY_PATH
        cmd += " --target %s" % self.target
        cmd += " --pidfile %s" % self.pidfile_path
        cmd += " --log %s" % self.log_path
        cmd += " --control-mode 666"
        cmd += " --verbose"
        cmd += " %s" % self.mirror
        cmd += " &> %s" % self.stdout_stderr_path
        node.account.ssh(cmd)
        util.wait_until(lambda: self.kibosh_running(node), 20, backoff_sec=.1,
                        err_msg="Timed out waiting for kibosh to start on %s" % node.account.hostname)

    def pids(self, node):
        return [pid for pid in node.account.ssh_capture("test -e '%s' && test -e /proc/$(cat '%s')" %
                                                        (self.pidfile_path, self.pidfile_path), allow_fail=True)]

    def wait_node(self, node, timeout_sec=None):
        return len(self.pids(node)) == 0

    def kibosh_process_running(self, node):
        pids = self.pids(node)
        if len(pids) == 0:
            return True
        return False

    def stop_node(self, node):
        """Halt kibosh process(es) on this node."""
        node.account.logger.debug("stop_node(%s): unmounting %s" % (node.name, self.mirror))
        node.account.ssh("sudo fusermount -u %s" % self.mirror, allow_fail=True)
        # Wait for the kibosh process to terminate.
        try:
            util.wait_until(lambda: self.kibosh_process_running(node), 20, backoff_sec=.1,
                            err_msg="Timed out waiting for kibosh to stop on %s" % node.account.hostname)
        except TimeoutError:
            # If the process won't terminate, use kill -9 to shut it down.
            node.account.logger.debug("stop_node(%s): killing the kibosh process managing %s" % (node.name, self.mirror))
            node.account.ssh("sudo kill -9 %s" % (" ".join(self.pids(node))), allow_fail=True)
            node.account.ssh("sudo fusermount -u %s" % self.mirror)
            util.wait_until(lambda: self.kibosh_process_running(node), 20, backoff_sec=.1,
                            err_msg="Timed out waiting for kibosh to stop on %s" % node.account.hostname)

    def clean_node(self, node):
        """Clean up persistent state on this node - e.g. service logs, configuration files etc."""
        self.stop_node(node)
        node.account.ssh("rm -rf -- %s" % self.persist)

    def set_faults(self, node, specs):
        """
        Set the currently active faults.

        :param node:        The node.
        :param spec:        An array of FaultSpec objects describing the faults.
        """
        fault_array = [spec.kibosh_message for spec in specs]
        obj = { 'faults': fault_array }
        obj_json = json.dumps(obj)
        node.account.create_file(self.control_path, obj_json)

    def get_fault_json(self, node):
        """
        Return a JSON string which contains the currently active faults.

        :param node:        The node.

        :returns:           The fault JSON describing the faults.
        """
        iter = node.account.ssh_capture("cat '%s'" % self.control_path)
        text = ""
        for line in iter:
            text = "%s%s" % (text, line.rstrip("\r\n"))
        return text
