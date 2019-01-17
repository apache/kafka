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

if __name__ == '__main__':
    print(json.dumps({"status":"ExternalCommandExample.py started."}))
    sys.stdout.flush()

    for line in sys.stdin:
        try:
            comm = json.loads(line)
            if "action" in comm:
                action = comm["action"]
                if action == "stop":
                    exit(0)
                elif action == "start":
                    if "spec" in comm:
                        task = comm["spec"]
                        print(json.dumps({"status":"Started to execute the workload."}))
                        sys.stdout.flush()
                        time.sleep(5)
                        print(json.dumps({"error":"Something is wrong.",
                                          "log":"Please show this line.",
                                          "status":{"totalSent":50000,"averageLatencyMs":10.83734,"p50LatencyMs":8,"p95LatencyMs":38,"p99LatencyMs":61,"transactionsCommitted":0}}))
                        sys.stdout.flush()
                        exit(0)
                    else:
                        print(json.dumps({"error": "No spec in command " + line}))
                        sys.stdout.flush()

                else:
                    print(json.dumps({"log": "Unknown command action " + action}))
            else:
                print(json.dumps({"error": "Unknown control command " + line }))
                sys.stdout.flush();
        except ValueError:
            print(json.dumps({"log": "Input line " + line + " is not a valid external runner command."}))