#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
import unittest
from pathlib import Path


CONFIGURE_SCRIPT = Path(__file__).parents[1] / "resources" / "common-scripts" / "configure"


class CommonScriptsTest(unittest.TestCase):
    @staticmethod
    def run_configure(**environment):
        env = {"PATH": os.environ["PATH"]}
        env.update(environment)
        return subprocess.run(
            ["bash", "-u", str(CONFIGURE_SCRIPT)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_missing_required_variable_is_reported_with_nounset(self):
        result = self.run_configure(KAFKA_PROCESS_ROLES="controller")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            result.stdout,
            "Running in KRaft mode...\nCLUSTER_ID environment variable not set\n",
        )
        self.assertEqual(result.stderr, "")

    def test_empty_required_variable_is_reported_with_nounset(self):
        result = self.run_configure(KAFKA_PROCESS_ROLES="controller", CLUSTER_ID="")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            result.stdout,
            "Running in KRaft mode...\nCLUSTER_ID environment variable not set\n",
        )
        self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
