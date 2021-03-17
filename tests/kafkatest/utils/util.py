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


def annotate_missing_msgs(missing, acked, consumed, msg):
    missing_list = list(missing)
    msg += "%s acked message did not make it to the Consumer. They are: " %\
        len(missing_list)
    if len(missing_list) < 20:
        msg += str(missing_list) + ". "
    else:
        msg += ", ".join(str(m) for m in missing_list[:20])
        msg += "...plus %s more. Total Acked: %s, Total Consumed: %s. " \
            % (len(missing_list) - 20, len(set(acked)), len(set(consumed)))
    return msg

def annotate_data_lost(data_lost, msg, number_validated):
    print_limit = 10
    if len(data_lost) > 0:
        msg += "The first %s missing messages were validated to ensure they are in Kafka's data files. " \
            "%s were missing. This suggests data loss. Here are some of the messages not found in the data files: %s\n" \
            % (number_validated, len(data_lost), str(data_lost[0:print_limit]) if len(data_lost) > print_limit else str(data_lost))
    else:
        msg += "We validated that the first %s of these missing messages correctly made it into Kafka's data files. " \
            "This suggests they were lost on their way to the consumer." % number_validated
    return msg

def validate_delivery(acked, consumed, idempotence_enabled=False, check_lost_data=None, may_truncate_acked_records=False):
    """Check that each acked message was consumed."""
    success = True
    msg = ""

    # Correctness of the set difference operation depends on using equivalent
    # message_validators in producer and consumer
    missing = set(acked) - set(consumed)
    
    # Were all acked messages consumed?
    if len(missing) > 0:
        msg = annotate_missing_msgs(missing, acked, consumed, msg)
        
        # Did we miss anything due to data loss?
        if check_lost_data:
            max_truncate_count = 100 if may_truncate_acked_records else 0
            max_validate_count = max(1000, max_truncate_count)

            to_validate = list(missing)[0:min(len(missing), max_validate_count)]
            data_lost = check_lost_data(to_validate)

            # With older versions of message format before KIP-101, data loss could occur due to truncation.
            # These records won't be in the data logs. Tolerate limited data loss for this case.
            if len(missing) < max_truncate_count and len(data_lost) == len(missing):
               msg += "The %s missing messages were not present in Kafka's data files. This suggests data loss " \
                   "due to truncation, which is possible with older message formats and hence are ignored " \
                   "by this test. The messages lost: %s\n" % (len(data_lost), str(data_lost))
            else:
                msg = annotate_data_lost(data_lost, msg, len(to_validate))
                success = False
        else:
            success = False

    # Are there duplicates?
    if len(set(consumed)) != len(consumed):
        num_duplicates = abs(len(set(consumed)) - len(consumed))

        if idempotence_enabled:
            success = False
            msg += "Detected %d duplicates even though idempotence was enabled.\n" % num_duplicates
        else:
            msg += "(There are also %d duplicate messages in the log - but that is an acceptable outcome)\n" % num_duplicates

    return success, msg
