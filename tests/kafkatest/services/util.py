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

from contextlib import contextmanager
from ducktape.utils.util import wait_until
import subprocess

def ssh_output(account, cmd, **kwargs):
    return "\n".join(list(account.ssh_capture(cmd, **kwargs)))

@contextmanager
def monitor_log(node, log):
    """
    Context manager that returns an object that helps you wait for events to
    occur in a log. This checks the size of the log at the beginning of the
    block and makes a helper object available with convenience methods for
    checking or waiting for a pattern to appear in the log. This will commonly
    be used to start a process, then wait for a log message indicating the
    process is in a ready state.

    See LogMonitor for more usage information.
    """
    try:
        offset = int(ssh_output(node.account, "wc -c %s" % log).split()[0])
    except subprocess.CalledProcessError:
        offset = 0
    yield LogMonitor(node, log, offset)

class LogMonitor(object):
    """
    Helper class returned by monitor_log. Should be used as:

    with monitor_log(node, "/path/to/log") as monitor:
        node.ssh("/command/to/start")
        monitor.wait_until("pattern.*to.*grep.*for", timeout_sec=5)

    to run the command and then wait for the pattern to appear in the log.
    """

    def __init__(self, node, log, offset):
        self.node = node
        self.log = log
        self.offset = offset

    def wait_until(self, pattern, **kwargs):
        """
        Wait until the specified pattern is found in the log, after the initial
        offset recorded when the LogMonitor was created. Additional keyword args
        are passed directly to ducktape.utils.util.wait_until
        """
        return wait_until(lambda: self.node.account.ssh("tail -c +%d %s | grep '%s'" % (self.offset+1, self.log, pattern), allow_fail=True) == 0, **kwargs)
