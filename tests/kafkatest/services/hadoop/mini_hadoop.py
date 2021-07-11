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

import os

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.tests.test import Test
from kafkatest.utils.remote_account import file_exists
from ducktape.utils.util import wait_until


class MiniHadoop(Service):
    DATANODE = "datanode"
    NAMENODE = "namenode"
    HADOOP_INSTALL_ROOT = "/opt"
    PERSISTENT_ROOT = "/mnt/hadoop"

    HADOOP_HOME = os.path.join(HADOOP_INSTALL_ROOT, "hadoop-3.2.2")
    HADOOP_CONF_DIR = os.path.join(PERSISTENT_ROOT, "config", "etc", "hadoop")
    HADOOP_NAME_DIR = os.path.join(PERSISTENT_ROOT, "data-logs", "dfs", "name")
    HADOOP_DATA_DIR = os.path.join(PERSISTENT_ROOT, "data-logs", "dfs", "data")
    HADOOP_PID_DIR = os.path.join(PERSISTENT_ROOT, "pid")
    HADOOP_LOG_DIR = os.path.join(PERSISTENT_ROOT, "operational-logs")

    logs = {
        "logs": {
            "path": HADOOP_LOG_DIR,
            "collect_default": True
        },
        "data": {
            "path": HADOOP_DATA_DIR,
            "collect_default": False
        }
    }

    def __init__(self, test_context, num_nodes):
        super(MiniHadoop, self).__init__(test_context, num_nodes)

        self.namenode = self.nodes[0]
        self.namenode_service_port = 8020
        self.namenode_web_port = 9870
        self.datanode_web_port = 9864
        self.log_level = "INFO"
        self.user = self.find_user()

    def stop(self):
        super(MiniHadoop, self).stop()
        self.stop_namenode(self.namenode)

    def start_node(self, node):
        self.context.logger.info("Starting the mini hadoop node %s", node.account.hostname)
        node.account.mkdirs(self.PERSISTENT_ROOT)
        self.configure_node(node)
        if node == self.namenode:
            self.start_namenode(node)
        self.start_datanode(node)

    def stop_node(self, node, clean_shutdown=True, shutdown_namenode=False):
        pids = self.pids(node)
        if self.DATANODE in pids.keys():
            self.stop_datanode(node)
        if shutdown_namenode and self.NAMENODE in pids.keys():
            self.stop_namenode(node)

        if clean_shutdown:
            self.wait_node(node, 60, shutdown_namenode)

    def wait_node(self, node, timeout_sec=None, check_namenode=False):
        for proc_name, pid in self.pids(node).items():
            if proc_name != self.NAMENODE or check_namenode:
                wait_until(lambda: not node.account.alive(pid), timeout_sec=timeout_sec,
                           err_msg="Hadoop process %s on %s took too long to exit" % (proc_name, node.account.hostname))

    def clean_node(self, node):
        for class_name in self.java_class_names():
            node.account.kill_java_processes(class_name, clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf -- %s" % MiniHadoop.PERSISTENT_ROOT)

    def start_namenode(self, node):
        # format the name node directory, if the directory doesn't exists.
        if not file_exists(node, self.HADOOP_DATA_DIR):
            node.account.mkdirs(self.HADOOP_DATA_DIR)
            file_name = os.path.join(self.HADOOP_LOG_DIR, "namenode-formatter.log")
            cmd = "echo y | %s --config %s namenode -format > %s 2>&1" % \
                  (self.path_script("hdfs"), self.HADOOP_CONF_DIR, file_name)
            self.logger.info("Attempting to format the namenode %s data directory with command %s",
                             str(node.account), cmd)
            with node.account.monitor_log(file_name) as monitor:
                node.account.ssh(cmd)
                pattern = "Storage directory.*has been successfully formatted"
                monitor.wait_until(pattern, timeout_sec=30, backoff_sec=.5,
                                   err_msg="Formatting namenode storage directory failed")

        # start the name node
        cmd = "%s --config %s --daemon start namenode" % (self.path_script("hdfs"), self.HADOOP_CONF_DIR)
        self.logger.info("Attempting to start the namenode %s with command %s", str(node.account), cmd)
        node.account.ssh(cmd)
        wait_until(lambda: self.health_check(node, self.namenode_web_port), timeout_sec=30, backoff_sec=.5,
                   err_msg="Couldn't able to start the namenode server")

    def stop_namenode(self, node):
        cmd = "%s --config %s --daemon stop namenode" % (self.path_script("hdfs"), self.HADOOP_CONF_DIR)
        self.logger.info("Attempting to stop the namenode in host: %s with cmd: %s", node.account.hostname, cmd)
        node.account.ssh(cmd, allow_fail=True)

    def start_datanode(self, node):
        cmd = "%s --config %s --daemon start datanode" % (self.path_script("hdfs"), self.HADOOP_CONF_DIR)
        self.logger.info("Attempting to start the datanode %s with command %s", str(node.account), cmd)
        node.account.ssh(cmd)
        wait_until(lambda: self.health_check(node, self.datanode_web_port), timeout_sec=30, backoff_sec=.5,
                   err_msg="Datanode server not started")

    def stop_datanode(self, node):
        cmd = "%s --config %s --daemon stop datanode" % (self.path_script("hdfs"), self.HADOOP_CONF_DIR)
        node.account.ssh(cmd)

    def namenode_uri(self):
        return "hdfs://%s:%d" % (self.namenode.account.hostname, self.namenode_service_port)

    def find_user(self):
        return [line.strip() for line in self.namenode.account.ssh_capture("whoami", callback=str)][0]

    @staticmethod
    def java_class_names():
        return ["org.apache.hadoop.hdfs.server.namenode.NameNode", "org.apache.hadoop.hdfs.server.datanode.DataNode"]

    def pids(self, node):
        result = {}
        for proc_name in [self.DATANODE, self.NAMENODE]:
            for pid in node.account.ssh_capture(
                    "cat %s" % self.pid_file(proc_name), allow_fail=True, callback=int, combine_stderr=False):
                result[proc_name] = pid
        return result

    def pid_file(self, proc_name):
        return os.path.join(self.HADOOP_PID_DIR, "hadoop-%s-%s.pid" % (self.user, proc_name))

    @staticmethod
    def health_check(node, port):
        cmd = "curl -f %s:%d" % (node.account.hostname, port)
        try:
            status = node.account.ssh(cmd)
            return status == 0
        except RemoteCommandError:
            return False

    def configure_node(self, node):
        node.account.mkdirs(self.HADOOP_CONF_DIR)
        node.account.mkdirs(self.HADOOP_LOG_DIR)
        node.account.ssh("cp -r %s %s" % (os.path.join(self.HADOOP_HOME, "etc", "hadoop", "*"), self.HADOOP_CONF_DIR))
        names = ["core-site.xml", "hdfs-site.xml", "hadoop-env.sh", "workers"]
        node.account.create_file(os.path.join(self.HADOOP_CONF_DIR, names[0]), self.core_site_file(names[0]))
        node.account.create_file(os.path.join(self.HADOOP_CONF_DIR, names[1]), self.hdfs_site_file(names[1]))
        node.account.create_file(os.path.join(self.HADOOP_CONF_DIR, names[2]), self.hadoop_env_file(node, names[2]))
        node.account.create_file(os.path.join(self.HADOOP_CONF_DIR, names[3]), self.datanode_workers())

    def core_site_file(self, template_name):
        return self.render(template_name)

    def hdfs_site_file(self, template_name):
        kwargs = {
            "hadoop_name_dir": self.HADOOP_NAME_DIR,
            "hadoop_data_dir": self.HADOOP_DATA_DIR
        }
        return self.render(template_name, **kwargs)

    def hadoop_env_file(self, node, template_name):
        kwargs = {
            "JAVA_HOME": self.java_home(node),
            "HADOOP_HOME": self.HADOOP_HOME,
            "HADOOP_CONF_DIR": self.HADOOP_CONF_DIR,
            "HADOOP_LOG_DIR": self.HADOOP_LOG_DIR,
            "HADOOP_PID_DIR": self.HADOOP_PID_DIR,
            "HADOOP_DAEMON_ROOT_LOGGER": self.log_level + ",RFA",
            "HADOOP_OS_TYPE": "${HADOOP_OS_TYPE:-$(uname -s)}",
            "HADOOP_GC_SETTINGS": "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps",
            "HDFS_NAMENODE_OPTS": "${HADOOP_GC_SETTINGS} -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M') "
                                  "-Dhadoop.security.logger=INFO,RFAS",
            "HADOOP_HEAPSIZE_MAX": "256m"
        }
        return self.render(template_name, kwargs=kwargs)

    def datanode_workers(self):
        # This is only required when using the utility script sbin/start-dfs.sh to start/stop all the data node at once.
        workers = "\n"
        hostnames = [node.account.hostname for node in self.nodes]
        return workers.join(hostnames)

    @staticmethod
    def java_home(node):
        lines = node.account.ssh_capture(
            "java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home'")
        for line in lines:
            if line.strip().startswith("java.home"):
                java_home = line.split("=", 1)[1].rsplit("/", 1)[0].strip()
                return java_home

    def path_script(self, name):
        return os.path.join(self.HADOOP_HOME, "bin", name)