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
import random
import uuid
from io import open
from os import remove, close
from shutil import move
from tempfile import mkstemp

from ducktape.services.service import Service

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin, CORE_LIBS_JAR_NAME, CORE_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.version import DEV_BRANCH


class MiniKdc(KafkaPathResolverMixin, Service):

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

    LOCAL_KEYTAB_FILE = None
    LOCAL_KRB5CONF_FILE = None

    @staticmethod
    def _set_local_keytab_file(local_scratch_dir):
        """Set MiniKdc.LOCAL_KEYTAB_FILE exactly once per test.

        LOCAL_KEYTAB_FILE is currently used like a global variable to provide a mechanism to share the
        location of the local keytab file among all services which might need it.

        Since individual ducktape tests are each run in a subprocess forked from the ducktape main process,
        class variables set at class load time are duplicated between test processes. This leads to collisions
        if test subprocesses are run in parallel, so we defer setting these class variables until after the test itself
        begins to run.
        """
        if MiniKdc.LOCAL_KEYTAB_FILE is None:
            MiniKdc.LOCAL_KEYTAB_FILE = os.path.join(local_scratch_dir, "keytab")
        return MiniKdc.LOCAL_KEYTAB_FILE

    @staticmethod
    def _set_local_krb5conf_file(local_scratch_dir):
        """Set MiniKdc.LOCAL_KRB5CONF_FILE exactly once per test.

        See _set_local_keytab_file for details why we do this.
        """

        if MiniKdc.LOCAL_KRB5CONF_FILE is None:
            MiniKdc.LOCAL_KRB5CONF_FILE = os.path.join(local_scratch_dir, "krb5conf")
        return MiniKdc.LOCAL_KRB5CONF_FILE

    def __init__(self, context, kafka_nodes, extra_principals=""):
        super(MiniKdc, self).__init__(context, 1)
        self.kafka_nodes = kafka_nodes
        self.extra_principals = extra_principals

        # context.local_scratch_dir uses a ducktape feature:
        # each test_context object has a unique local scratch directory which is available for the duration of the test
        # which is automatically garbage collected after the test finishes
        MiniKdc._set_local_keytab_file(context.local_scratch_dir)
        MiniKdc._set_local_krb5conf_file(context.local_scratch_dir)

    def replace_in_file(self, file_path, pattern, subst):
        fh, abs_path = mkstemp()
        with open(abs_path, 'w') as new_file:
            with open(file_path) as old_file:
                for line in old_file:
                    new_file.write(line.replace(pattern, subst))
        close(fh)
        remove(file_path)
        move(abs_path, file_path)

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % MiniKdc.WORK_DIR, allow_fail=False)
        props_file = self.render('minikdc.properties',  node=node)
        node.account.create_file(MiniKdc.PROPS_FILE, props_file)
        self.logger.info("minikdc.properties")
        self.logger.info(props_file)

        kafka_principals = ' '.join(['kafka/' + kafka_node.account.hostname for kafka_node in self.kafka_nodes])
        principals = 'client ' + kafka_principals + ' ' + self.extra_principals
        self.logger.info("Starting MiniKdc with principals " + principals)

        core_libs_jar = self.path.jar(CORE_LIBS_JAR_NAME, DEV_BRANCH)
        core_dependant_test_libs_jar = self.path.jar(CORE_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)

        cmd = "for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_dependant_test_libs_jar
        cmd += " export CLASSPATH;"
        cmd += " %s kafka.security.minikdc.MiniKdc %s %s %s %s 1>> %s 2>> %s &" % (self.path.script("kafka-run-class.sh", node), MiniKdc.WORK_DIR, MiniKdc.PROPS_FILE, MiniKdc.KEYTAB_FILE, principals, MiniKdc.LOG_FILE, MiniKdc.LOG_FILE)
        self.logger.debug("Attempting to start MiniKdc on %s with command: %s" % (str(node.account), cmd))
        with node.account.monitor_log(MiniKdc.LOG_FILE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until("MiniKdc Running", timeout_sec=60, backoff_sec=1, err_msg="MiniKdc didn't finish startup")

        node.account.copy_from(MiniKdc.KEYTAB_FILE, MiniKdc.LOCAL_KEYTAB_FILE)
        node.account.copy_from(MiniKdc.KRB5CONF_FILE, MiniKdc.LOCAL_KRB5CONF_FILE)

        # KDC is set to bind openly (via 0.0.0.0). Change krb5.conf to hold the specific KDC address
        self.replace_in_file(MiniKdc.LOCAL_KRB5CONF_FILE, '0.0.0.0', node.account.hostname)

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


