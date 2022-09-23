# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re


def verify_running(processor, message):
    node = processor.node
    with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
        processor.start()
        wait_for(monitor, processor, message)

def verify_stopped(processor, message):
    node = processor.node
    with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
        processor.stop()
        wait_for(monitor, processor, message)

def stop_processors(processors, stopped_message):
    for processor in processors:
        verify_stopped(processor, stopped_message)

def extract_generation_from_logs(processor):
    return list(processor.node.account.ssh_capture("grep \"Successfully joined group with generation\" %s| awk \'{for(i=1;i<=NF;i++) {if ($i == \"generation\") beginning=i+1; if($i== \"(org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)\") ending=i }; for (j=beginning;j<ending;j++) printf $j; printf \"\\n\"}\'" % processor.LOG_FILE, allow_fail=True))

def extract_generation_id(generation):
    # Generation string looks like
    # "Generation{generationId=5,memberId='consumer-A-3-72d7be15-bcdd-4032-b247-784e648d4dd8',protocol='stream'} "
    # Extracting generationId from it.
    m = re.search(r'Generation{generationId=(\d+),.*', generation)
    return int(m.group(1))

def wait_for(monitor, processor, output, timeout_sec=60):
    monitor.wait_until(output,
                       timeout_sec=timeout_sec,
                       err_msg=("Never saw output '%s' on " % output) + str(processor.node.account))