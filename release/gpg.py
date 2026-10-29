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
Auxiliary functions to interact with GNU Privacy Guard (GPG).
"""

import hashlib
import subprocess
import tempfile

from runtime import execute


def key_exists(key_id):
    """
    Checks whether the specified GPG key exists locally.
    """
    try:
        execute(f"gpg --list-keys {key_id}")
    except Exception:
        return False
    return True


def agent_kill():
    """
    Tries to kill the GPG agent process.
    """
    try:
        execute("gpgconf --kill gpg-agent")
    except FileNotFoundError as e:
        if e.filename != 'gpgconf':
            raise e


def sign(key_id, passphrase, content, target):
    """
    Generates a GPG signature, using the given key and passphrase,
    of the specified content into the target path.
    """
    execute(f"gpg --passphrase-fd 0 -u {key_id} --armor --output {target} --detach-sig {content}", input=passphrase.encode())


def verify(content, signature):
    """
    Verify the given GPG signature for the specified content.
    """
    execute(f"gpg --verify {signature} {content}")


def valid_passphrase(key_id, passphrase):
    """
    Checks whether the given passphrase is workable for the given key.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        content = __file__
        signature = tmpdir + '/sig.asc'
        # if the agent is running, the supplied passphrase may be ignored
        agent_kill()
        try:
            sign(key_id, passphrase, content, signature)
            verify(content, signature)
        except subprocess.CalledProcessError:
            return False
    return True


def key_pass_id(key_id, passphrase):
    """
    Generates a deterministic identifier for the key and passphrase combination.
    """
    h = hashlib.sha512()
    h.update(key_id.encode())
    h.update(passphrase.encode())
    return h.hexdigest()
