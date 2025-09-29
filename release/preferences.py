#
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
#

"""
Access and manage name&value preferences, persisted in a local JSON file.
"""

import os
import json

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
FILE = os.path.join(THIS_DIR, ".release-settings.json")


prefs = {}
if os.path.exists(FILE):
    with open(FILE, "r") as prefs_fp:
        prefs = json.load(prefs_fp)
    if len(prefs) > 0:
        print(f"Using preferences from: {FILE}")


def save():
    """
    Save preferences dictionary.
    """
    print(f"Saving preferences to {FILE}")
    with open(FILE, "w") as prefs_fp:
        json.dump(prefs, prefs_fp)


def set(name, val):
    """
    Store and persist a preference.
    """
    prefs[name] = val
    save()


def unset(name):
    """
    Removes a preference.
    """
    del prefs[name]
    save()


def get(name, supplier):
    """
    Retrieve preference if it already exists or delegate
    to the given value supplier and store the result.
    """
    val = prefs.get(name)
    if not val:
        val = supplier()
        set(name, val)
    else:
        print(f"Assuming: {name} = {val}")
    return val


def once(name, action):
    """
    Performs the given action if and only if no record of it
    having been executed before exists in the preferences dictionary.
    """
    def run_action():
        action()
        return True
    get(f"did_{name}", run_action)


def as_json():
    """
    Export all saved preferences in JSON format.
    """
    json.dumps(prefs, indent=2)
