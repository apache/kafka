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

from kafkatest.services.trogdor.fault_spec import FaultSpec


class NoOpFaultSpec(FaultSpec):
    """
    The specification for a nop-op fault.

    No-op faults are used to test the fault injector.  They don't do anything,
    but must be propagated to all fault injector daemons.
    """

    def __init__(self, start_ms, duration_ms):
        """
        Create a new NoOpFault.

        :param start_ms:        The start time, as described in fault_spec.py
        :param duration_ms:     The duration in milliseconds.
        """
        super(NoOpFaultSpec, self).__init__(start_ms, duration_ms)

    def message(self):
        return {
            "class": "org.apache.kafka.trogdor.fault.NoOpFaultSpec",
            "startMs": self.start_ms,
            "durationMs": self.duration_ms,
        }
