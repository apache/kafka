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

class NetworkFracture(object):
    """
    A fault which divides a network into multiple parts which cannot communicate with each other.

    :param groups: An array of arrays of nodes.  Each node must be in only one group.
    """
    def __init__(self, logger, groups):
        self.logger = logger
        self.groups = groups
        if len(self.groups) < 2:
            raise RuntimeError("You must supply at least 2 groups of nodes.")
        for group_a in self.groups:
            for node_a in group_a:
                for group_b in self.groups:
                    if group_b == group_a:
                        continue
                    for node_b in group_b:
                        if node_a == node_b:
                            raise RuntimeError("Node " + str(node_a) + " is in more than one group!" +
                                               "Nodes must be in only one group.")
        self.logger.debug("Created %s" % str(self))

    def start(self):
        for group_a in self.groups:
            for node_a in group_a:
                rules = []
                for group_b in self.groups:
                    if group_a == group_b:
                        continue
                    for node_b in group_b:
                        rules.append("sudo iptables -A INPUT -p tcp -s %s -j DROP" %
                                 self.node_to_ip_address(node_b))
                cmd = " && ".join(rules)
                self.logger.debug("%s: %s" % (self.node_to_ip_address(node_a), cmd))
                node_a.account.ssh(cmd)

    def node_to_ip_address(self, node):
        return node.account.externally_routable_ip

    def stop(self):
        for group in self.groups:
            for node in group:
                cmd = "sudo iptables -F INPUT"
                self.logger.debug("%s: %s" % (self.node_to_ip_address(node), cmd))
                node.account.ssh(cmd)

    def __str__(self):
        description = "["
        group_prefix = ""
        for group in self.groups:
            description += group_prefix
            group_prefix += ", "
            prefix = ""
            description += "["
            for node in group:
                description += prefix
                prefix = ", "
                description += self.node_to_ip_address(node)
            description += "]"
        description += "]"
        return "NetworkFracture(%s)" % description
