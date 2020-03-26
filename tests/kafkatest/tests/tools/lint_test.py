import os
import shlex
import sys
from io import StringIO
from subprocess import PIPE, Popen, call

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

class PylintTest(Test):

    @cluster(num_nodes=1)
    def test_pylint(self):
        """
        Run pylint against kafkatest

        This code is lifted from pylint.epylint. We had to copy the code into here so we could change directories
        before running
        """

        executable = sys.executable if "python" in sys.executable else "python"
        # Create command line to call pylint
        epylint_part = [executable, "-c", "from pylint import epylint;epylint.Run()"]
        options = shlex.split("kafkatest -E", posix=not sys.platform.startswith("win"))
        cli = epylint_part + options
        cwd = os.path.join(os.getcwd(), "tests")
        # Call pylint in a subprocess
        process = Popen(
            cli,
            shell=False,
            cwd=cwd,
            stdout=PIPE,
            stderr=PIPE,
            env=_get_env(),
            universal_newlines=True,
        )
        out, err = process.communicate()

        if out != "":
            self.logger.warn("Had pylint errors")
            self.logger.warn(out)
            assert False, "Had pyline errors, see stdout"


def _get_env():
    """Extracts the environment PYTHONPATH and appends the current sys.path to
    those."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    return env
