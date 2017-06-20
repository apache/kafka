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


from kafkatest.fault.network_fracture import NetworkFracture


class DummyAccount(object):
    def __init__(self, ip):
        self.externally_routable_ip = ip
        self.cmds = []

    def ssh(self, cmd):
        self.cmds.append(cmd)

class DummyNode(object):
    def __init__(self, ip):
        self.account = DummyAccount(ip)

class DummyLogger(object):
    def __init__(self):
        pass
    def debug(self, msg):
        pass
    def info(self, msg):
        pass

class CheckNetworkFracture(object):
    def check_init(self):
        node_a = DummyNode("127.0.0.1")
        node_b = DummyNode("127.0.1.1")
        node_c = DummyNode("127.0.1.0")
        NetworkFracture(DummyLogger(), [[node_a], [node_b, node_c]])
        NetworkFracture(DummyLogger(), [[node_a, node_b], [node_c]])
        NetworkFracture(DummyLogger(), [[node_a, node_b, node_c], []])

        try:
            NetworkFracture(DummyLogger(), [[node_a, node_b, node_c]])
            assert False
        except RuntimeError as e:
            assert e.message.find("You must supply at least 2 groups of nodes.") != -1

        try:
            NetworkFracture(DummyLogger(), [[node_a, node_b], [node_b]])
            assert False
        except RuntimeError as e:
            assert e.message.find("is in more than one group!") != -1

        try:
            NetworkFracture(DummyLogger(), [[node_a, node_c], [node_b, node_c]])
            assert False
        except RuntimeError as e:
            assert e.message.find("is in more than one group!") != -1

    def check_start(self):
        node_a = DummyNode("127.0.0.1")
        node_b = DummyNode("127.0.1.1")
        node_c = DummyNode("127.0.1.0")
        fracture = NetworkFracture(DummyLogger(), [[node_a], [node_b, node_c]])
        fracture.start()

        assert len(node_a.account.cmds) == 1
        node_a_expected = "sudo iptables -A INPUT -p tcp -s 127.0.1.1 -j DROP" + \
                          " && sudo iptables -A INPUT -p tcp -s 127.0.1.0 -j DROP"
        assert node_a.account.cmds[0] == node_a_expected

        assert len(node_b.account.cmds) == 1
        node_b_expected = "sudo iptables -A INPUT -p tcp -s 127.0.0.1 -j DROP"
        assert node_b.account.cmds[0] == node_b_expected

        assert len(node_c.account.cmds) == 1
        assert node_c.account.cmds[0] == node_b_expected

    def check_stop(self):
        node_a = DummyNode("127.0.0.1")
        node_b = DummyNode("127.0.1.1")
        node_c = DummyNode("127.0.1.0")
        fracture = NetworkFracture(DummyLogger(), [[node_a], [node_b, node_c]])
        fracture.stop()

        for node in [node_a, node_b, node_c]:
            assert len(node.account.cmds) == 1
            assert node.account.cmds[0] == "sudo iptables -F INPUT"
