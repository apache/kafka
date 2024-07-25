#!/usr/bin/env python

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

"""
Python script that houses the version-specific GPG keys. 
These keys are used to validate the kafka tarball before it is unpacked in the Docker images.
For each new version of Kafka that will be released, 
the version-specific GPG keys will be added to this file.

Usage:
    from docker.version_gpg_keys import version_gpg_keys

    This returns a python dict - version_gpg_keys, with keys indicating the Kafka version, 
    and the values as the GPG key for that specific version.
"""

version_gpg_keys = {
    '3.7.0': '7C38C2F6E7DF40E527C7C996DE0D9D12FB1360DA',
    '3.7.1': '4687E2BC1319B57B321D6F0E39AB5531A7FCB08E',
    '3.8.0': 'CF9500821E9557AEB04E026C05EEA67F87749E61'
}