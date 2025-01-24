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
Auxiliary functions to manage the release script runtime
and launch external utilities.
"""

import os
import subprocess
import sys
import tempfile

import templates


this_script_dir = os.path.abspath(os.path.dirname(__file__))
repo_dir = os.environ.get("KAFKA_HOME", os.path.abspath(this_script_dir + "/.."))

fail_hooks = []
failing = False


def append_fail_hook(name, hook_fn):
    """
    Register a fail hook function, to run in case fail() is called.
    """
    fail_hooks.append((name, hook_fn))


def fail(msg = ""):
    """
    Terminate execution with the given message,
    after running any registered hooks.
    """
    global failing
    if failing:
        raise Exception(f"Recursive fail invocation")
    failing = True

    for name, func in fail_hooks:
        try:
            func()
        except Exception as e:
            print(f"Exception caught in fail hook {name}: {e}")

    print(f"FAILURE: {msg}")
    sys.exit(1)


def prompt(msg):
    """
    Prompt user for input with the given message.
    This removes leading and trailing spaces.
    """
    text = input(msg)
    return text.strip()


def confirm(msg):
    """
    Prompt the user to confirm
    """
    while True:
        text = prompt(msg + " (y/n): ").lower()
        if text in ['y', 'n']:
            return text == 'y'


def confirm_or_fail(msg):
    """
    Prompt the user to confirm and fail on negative input.
    """
    if not confirm(msg):
        fail("Ok, giving up")


def execute(cmd, *args, **kwargs):
    """
    Execute an external command and return its output.
    """
    if "shell" not in kwargs and isinstance(cmd, str):
        cmd = cmd.split()
    if "input" in kwargs and isinstance(kwargs["input"], str):
        kwargs["input"] = kwargs["input"].encode()
    kwargs["stderr"] = stderr=subprocess.STDOUT
    output = subprocess.check_output(cmd, *args, **kwargs)
    return output.decode("utf-8")


def _prefix(prefix_str, value_str):
    return prefix_str + value_str.replace("\n", "\n" + prefix_str)


def cmd(action, cmd_arg, *args, **kwargs):
    """
    Execute an external command. This should be preferered over execute()
    when returning the output is not necessary, as the user will be given
    the option of retrying in case of a failure.
    """
    stdin_log = ""
    if "stdin" in kwargs and isinstance(kwargs["stdin"], str):
        stdin_str = kwargs["stdin"]
        stdin_log = "\n" + _prefix("< ", stdin_str)
        stdin = tempfile.TemporaryFile()
        stdin.write(stdin_str.encode("utf-8"))
        stdin.seek(0)
        kwargs["stdin"] = stdin

    print(f"{action}\n$ {cmd_arg}{stdin_log}")

    if isinstance(cmd_arg, str) and not kwargs.get("shell", False):
        cmd_arg = cmd_arg.split()

    allow_failure = kwargs.pop("allow_failure", False)

    retry = True
    while retry:
        try:
            output = execute(cmd_arg, *args, stderr=subprocess.STDOUT, **kwargs)
            print(_prefix("> ", output.strip()))
            return True
        except subprocess.CalledProcessError as e:
            print(e.output.decode("utf-8"))

            if allow_failure:
                return False

            retry = confirm("Retry?")

    print(templates.cmd_failed())
    fail("")


