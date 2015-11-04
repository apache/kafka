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

from ducktape.services.service import Service
from kafkatest.services.kafka.directory import kafka_dir

import os


class MiniKdc(Service):

    logs = {
        "minikdc_log": {
            "path": "/mnt/minikdc/minikdc.log",
            "collect_default": True}
    }

    WORK_DIR = "/mnt/minikdc"
    PROPS_FILE = "/mnt/minikdc/minikdc.properties"
    KEYTAB_FILE = "/mnt/minikdc/keytab"
    KRB5CONF_FILE = "/mnt/minikdc/krb5.conf"
    LOG_FILE = "/mnt/minikdc/minikdc.log"
    LOCAL_KEYTAB_FILE = "/tmp/keytab"
    LOCAL_KRB5CONF_FILE = "/tmp/krb5.conf"

    def __init__(self, context, kafka_nodes):
        super(MiniKdc, self).__init__(context, 1)
        self.kafka_nodes = kafka_nodes


    def start_node(self, node):

        node.account.ssh("mkdir -p %s" % MiniKdc.WORK_DIR, allow_fail=False)
        props_file = self.render('minikdc.properties',  node=node)
        node.account.create_file(MiniKdc.PROPS_FILE, props_file)
        self.logger.info("minikdc.properties")
        self.logger.info(props_file)

        kafka_principals = ' '.join(['kafka/' + kafka_node.account.hostname for kafka_node in self.kafka_nodes])
        principals = 'client ' + kafka_principals
        self.logger.info("Starting MiniKdc with principals " + principals)

        lib_dir = "/opt/%s/core/build/dependant-testlibs" % kafka_dir(node)
        kdc_jars = node.account.ssh_capture("ls " + lib_dir)
        classpath = ":".join([os.path.join(lib_dir, jar.strip()) for jar in kdc_jars])
        cmd = "CLASSPATH=%s /opt/%s/bin/kafka-run-class.sh org.apache.hadoop.minikdc.MiniKdc %s %s %s %s 1>> %s 2>> %s &" % (classpath, kafka_dir(node), MiniKdc.WORK_DIR, MiniKdc.PROPS_FILE, MiniKdc.KEYTAB_FILE, principals, MiniKdc.LOG_FILE, MiniKdc.LOG_FILE)
        self.logger.debug("Attempting to start MiniKdc on %s with command: %s" % (str(node.account), cmd))
        with node.account.monitor_log(MiniKdc.LOG_FILE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until("MiniKdc Running", timeout_sec=60, backoff_sec=1, err_msg="MiniKdc didn't finish startup")
        node.account.scp_from(MiniKdc.KEYTAB_FILE, MiniKdc.LOCAL_KEYTAB_FILE)
        node.account.scp_from(MiniKdc.KRB5CONF_FILE, MiniKdc.LOCAL_KRB5CONF_FILE)


    def stop_node(self, node):
        self.logger.info("Stopping %s on %s" % (type(self).__name__, node.account.hostname))
        node.account.kill_process("apacheds", allow_fail=False)

    def clean_node(self, node):
        node.account.kill_process("apacheds", clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf " + MiniKdc.WORK_DIR, allow_fail=False)
        if os.path.exists(MiniKdc.LOCAL_KEYTAB_FILE):
            os.remove(MiniKdc.LOCAL_KEYTAB_FILE)
        if os.path.exists(MiniKdc.LOCAL_KRB5CONF_FILE):
            os.remove(MiniKdc.LOCAL_KRB5CONF_FILE)


