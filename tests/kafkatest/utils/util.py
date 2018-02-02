# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest import __version__ as __kafkatest_version__

import re
import time


def kafkatest_version():
    """Return string representation of current ducktape version."""
    return __kafkatest_version__


def _kafka_jar_versions(proc_string):
    """Use a rough heuristic to find all kafka versions explicitly in the process classpath. We need to handle patterns
    like:
        - kafka_2.11-1.0.0-SNAPSHOT.jar
        - kafka_2.11-0.11.0.0-SNAPSHOT.jar
        - kafka-1.0.0/bin/../libs/* (i.e. the JARs are not listed explicitly)
        - kafka-0.11.0.0/bin/../libs/* (i.e. the JARs are not listed explicitly)
        - kafka-streams-1.0.0-SNAPSHOT.jar
        - kafka-streams-0.11.0.0-SNAPSHOT.jar
    """

    # Pattern example: kafka_2.11-1.0.0-SNAPSHOT.jar (we have to be careful not to partially match the 4 segment version string)
    versions = re.findall("kafka_[0-9]+\.[0-9]+-([0-9]+\.[0-9]+\.[0-9]+)[\.-][a-zA-z]", proc_string)

    # Pattern example: kafka_2.11-0.11.0.0-SNAPSHOT.jar
    versions.extend(re.findall("kafka_[0-9]+\.[0-9]+-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", proc_string))

    # Pattern example: kafka-1.0.0/bin/../libs/* (i.e. the JARs are not listed explicitly, we have to be careful not to
    # partially match the 4 segment version)
    versions.extend(re.findall("kafka-([0-9]+\.[0-9]+\.[0-9]+)/", proc_string))

    # Pattern example: kafka-0.11.0.0/bin/../libs/* (i.e. the JARs are not listed explicitly)
    versions.extend(re.findall("kafka-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", proc_string))

    # Pattern example: kafka-streams-1.0.0-SNAPSHOT.jar (we have to be careful not to partially match the 4 segment version string)
    versions.extend(re.findall("kafka-[a-z]+-([0-9]+\.[0-9]+\.[0-9]+)[\.-][a-zA-z]", proc_string))

    # Pattern example: kafka-streams-0.11.0.0-SNAPSHOT.jar
    versions.extend(re.findall("kafka-[a-z]+-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", proc_string))

    return set(versions)


def is_version(node, version_list, proc_grep_string="kafka", logger=None):
    """Heuristic to check that only the specified version appears in the classpath of the process
    A useful tool to aid in checking that service version apis are working correctly.
    """
    lines = [l for l in node.account.ssh_capture("ps ax | grep %s | grep -v grep" % proc_grep_string)]
    assert len(lines) == 1
    psLine = lines[0]

    versions = _kafka_jar_versions(psLine)
    r = versions == {str(v) for v in version_list}
    if not r and logger is not None:
        logger.warning("%s: %s version mismatch: expected %s, actual %s, ps line %s" % \
                       (str(node), proc_grep_string, version_list, versions, psLine))
    return r


def is_int(msg):
    """Method used to check whether the given message is an integer

    return int or raises an exception if message is not an integer
    """
    try:
        return int(msg)
    except ValueError:
        raise Exception("Unexpected message format (expected an integer). Message: %s" % (msg))


def is_int_with_prefix(msg):
    """
    Method used check whether the given message is of format 'integer_prefix'.'integer_value'

    :param msg: message to validate
    :return: msg or raises an exception is a message is of wrong format
    """
    try:
        parts = msg.split(".")
        if len(parts) != 2:
            raise Exception("Unexpected message format. Message should be of format: integer "
                            "prefix dot integer value. Message: %s" % (msg))
        int(parts[0])
        int(parts[1])
        return msg
    except ValueError:
        raise Exception("Unexpected message format. Message should be of format: integer "
                        "prefix dot integer value, but one of the two parts (before or after dot) "
                        "are not integers. Message: %s" % (msg))

def node_is_reachable(src_node, dst_node):
    """
    Returns true if a node is unreachable from another node.

    :param src_node:        The source node to check from reachability from.
    :param dst_node:        The destination node to check for reachability to.
    :return:                True only if dst is reachable from src.
    """
    return 0 == src_node.account.ssh("nc -w 3 -z %s 22" % dst_node.account.hostname, allow_fail=True)
