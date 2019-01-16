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

import argparse
import json
import sys
import time

# ExternalCommandExample.py demonstrates how the external command process communicates with the agent.

parser = argparse.ArgumentParser(description='Python Trogdor External Command Example.')
parser.add_argument('--spec', dest='spec', required=True)
args = parser.parse_args()

if __name__ == '__main__':
    task = json.loads(args.spec)
    print(json.dumps({"status":"Python external command runner executes command  " + " ".join(task["command"])}))
    sys.stdout.flush()

    time.sleep(10)
    print(json.dumps({"error":"Something is wrong.",
                      "log":"Please show this line.",
                      "status":{"totalSent":50000,"averageLatencyMs":10.83734,"p50LatencyMs":8,"p95LatencyMs":38,"p99LatencyMs":61,"transactionsCommitted":0}}))
    sys.stdout.flush()

    for line in sys.stdin:
        print(json.dumps({"log":line}))
        sys.stdout.flush()
        try:
            comm = json.loads(line)
            if ("action" in comm and comm["action"] == "stop"):
                exit(0)
        except ValueError:
            print(json.dumps({"log": "Input line " + line + " is not a valid external runner command."}))