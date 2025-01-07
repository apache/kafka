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
from datetime import datetime


def pretty_time_duration(seconds: float) -> str:
    time_min, time_sec = divmod(int(seconds), 60)
    time_hour, time_min = divmod(time_min, 60)
    time_fmt = ""
    if time_hour > 0:
        time_fmt += f"{time_hour}h"
    if time_min > 0:
        time_fmt += f"{time_min}m"
    time_fmt += f"{time_sec}s"
    return time_fmt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Gradle log output to find hanging tests")
    parser.add_argument("file", type=argparse.FileType("r"), help="Text file containing Gradle stdout")
    args = parser.parse_args()

    started = dict()
    last_test_line = None
    for line in args.file.readlines():
        if "Gradle Test Run" not in line:
            continue
        last_test_line = line

        toks = line.strip().split(" > ")
        name, status = toks[-1].rsplit(" ", 1)
        name_toks = toks[2:-1] + [name]
        test = " > ".join(name_toks)
        if status == "STARTED":
            started[test] = line
        else:
            started.pop(test)

    last_timestamp, _ = last_test_line.split(" ", 1)
    last_dt = datetime.fromisoformat(last_timestamp)

    if len(started) > 0:
        print("Found tests that were started, but apparently not finished")

    for started_not_finished, line in started.items():
        print("-"*80)
        timestamp, _ = line.split(" ", 1)
        dt = datetime.fromisoformat(timestamp)
        dur_s = (last_dt - dt).total_seconds()
        print(f"Test: {started_not_finished}")
        print(f"Duration: {pretty_time_duration(dur_s)}")
        print(f"Raw line: {line}")
