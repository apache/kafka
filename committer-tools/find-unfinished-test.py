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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Gradle log output to find hanging tests")
    parser.add_argument("file", type=argparse.FileType("r"), help="Text file containing Gradle stdout")
    args = parser.parse_args()

    started = dict()
    for line in args.file.readlines():
        if "Gradle Test Run" not in line:
            continue
        toks = line.strip().split(" > ")
        name, status = toks[-1].rsplit(" ", 1)
        name_toks = toks[2:-1] + [name]
        test = " > ".join(name_toks)
        if status == "STARTED":
            started[test] = line
        else:
            started.pop(test)

    if len(started) > 0:
        print("Found tests that were started, but not finished:\n")

    for started_not_finished, line in started.items():
        print(line)
