#!/bin/sh
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

PIDFILE="${PIDFILE:-"/var/run/kafka.pid"}"
SIGNAL=${SIGNAL:-TERM}

if [ ! -e "${PIDFILE}" ]; then
  echo "pidfile does not exist. kafka may not be running"
  exit 1
fi

PID=$(cat "${PIDFILE}")

kill -s "$SIGNAL" "$PID"

# Check if process behind pid is still running and wait until its stopped
# See `man 2 kill` for explanation of `kill -0`
while kill -0 "${PID}" > /dev/null; do
  echo "waiting until server (pid ${PID}) is stopped"
  sleep 1
done

echo "server stopped"

rm -f "${PIDFILE}"

