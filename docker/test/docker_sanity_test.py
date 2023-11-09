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

import unittest
import subprocess
from HTMLTestRunner import HTMLTestRunner
import test.constants

class DockerSanityTest(unittest.TestCase):
    IMAGE="apache/kafka"
    
    def resume_container(self):
        subprocess.run(["docker", "start", test.constants.BROKER_CONTAINER])

    def stop_container(self) -> None:
        subprocess.run(["docker", "stop", test.constants.BROKER_CONTAINER])

    def start_compose(self, filename) -> None:
        old_string="image: {$IMAGE}"
        new_string=f"image: {self.IMAGE}"
        with open(filename) as f:
            s = f.read()
        with open(filename, 'w') as f:
            s = s.replace(old_string, new_string)
            f.write(s)

        subprocess.run(["docker-compose", "-f", filename, "up", "-d"])
    
    def destroy_compose(self, filename) -> None:
        old_string=f"image: {self.IMAGE}"
        new_string="image: {$IMAGE}"
        subprocess.run(["docker-compose", "-f", filename, "down"])
        with open(filename) as f:
            s = f.read()
        with open(filename, 'w') as f:
            s = s.replace(old_string, new_string)
            f.write(s)

    def create_topic(self, topic, topic_config):
        command = [test.constants.KAFKA_TOPICS, "--create", "--topic", topic]
        command.extend(topic_config)
        subprocess.run(command)
        check_command = [test.constants.KAFKA_TOPICS, "--list"]
        check_command.extend(topic_config)
        output = subprocess.check_output(check_command, timeout=test.constants.CLIENT_TIMEOUT)
        if topic in output.decode("utf-8"):
            return True
        return False
        
    def produce_message(self, topic, producer_config, key, value):
        command = ["echo", f'"{key}:{value}"', "|", test.constants.KAFKA_CONSOLE_PRODUCER, "--topic", topic, "--property", "'parse.key=true'", "--property", "'key.separator=:'"]
        command.extend(producer_config)
        subprocess.run(["bash", "-c", " ".join(command)], timeout=test.constants.CLIENT_TIMEOUT)
    
    def consume_message(self, topic, consumer_config):
        command = [test.constants.KAFKA_CONSOLE_CONSUMER, "--topic", topic, "--property", "'print.key=true'", "--property", "'key.separator=:'", "--from-beginning", "--max-messages", "1"]
        command.extend(consumer_config)
        message = subprocess.check_output(["bash", "-c", " ".join(command)], timeout=test.constants.CLIENT_TIMEOUT)
        return message.decode("utf-8").strip()

    def ssl_flow(self, ssl_broker_port, test_name, test_error_prefix, topic):
        print(f"Running {test_name}")
        errors = []
        try:
            self.assertTrue(self.create_topic(topic, ["--bootstrap-server", ssl_broker_port, "--command-config", test.constants.SSL_CLIENT_CONFIG]))
        except AssertionError as e:
            errors.append(test_error_prefix + str(e))
            return errors

        producer_config = ["--bootstrap-server", ssl_broker_port,
                           "--producer.config", test.constants.SSL_CLIENT_CONFIG]
        self.produce_message(topic, producer_config, "key", "message")

        consumer_config = [
            "--bootstrap-server", ssl_broker_port,
            "--property", "auto.offset.reset=earliest",
            "--consumer.config", test.constants.SSL_CLIENT_CONFIG,
        ]
        message = self.consume_message(topic, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(test_error_prefix + str(e))
        try:
            self.assertEqual(message, "key:message")
        except AssertionError as e:
            errors.append(test_error_prefix + str(e))
        if errors:
            print(f"Errors in {test_name}:- {errors}")
        else:
            print(f"No errors in {test_name}")
        return errors
    
    def broker_restart_flow(self):
        print(f"Running {test.constants.BROKER_RESTART_TESTS}")
        errors = []
        
        try:
            self.assertTrue(self.create_topic(test.constants.BROKER_RESTART_TEST_TOPIC, ["--bootstrap-server", "localhost:9092"]))
        except AssertionError as e:
            errors.append(test.constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        
        producer_config = ["--bootstrap-server", "localhost:9092", "--property", "client.id=host"]
        self.produce_message(test.constants.BROKER_RESTART_TEST_TOPIC, producer_config, "key", "message")

        print("Stopping Container")
        self.stop_container()
        print("Resuming Container")
        self.resume_container()

        consumer_config = ["--bootstrap-server", "localhost:9092", "--property", "auto.offset.reset=earliest"]
        message = self.consume_message(test.constants.BROKER_RESTART_TEST_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(test.constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        try:
            self.assertEqual(message, "key:message")
        except AssertionError as e:
            errors.append(test.constants.BROKER_RESTART_ERROR_PREFIX + str(e))
        if errors:
            print(f"Errors in {test.constants.BROKER_RESTART_TESTS}:- {errors}")
        else:
            print(f"No errors in {test.constants.BROKER_RESTART_TESTS}")
        return errors

    def execute(self):
        total_errors = []
        try:
            total_errors.extend(self.ssl_flow('localhost:9093', test.constants.SSL_FLOW_TESTS, test.constants.SSL_ERROR_PREFIX, test.constants.SSL_TOPIC))
        except Exception as e:
            print(test.constants.SSL_ERROR_PREFIX, str(e))
            total_errors.append(str(e))
        try:
            total_errors.extend(self.ssl_flow('localhost:9094', test.constants.FILE_INPUT_FLOW_TESTS, test.constants.FILE_INPUT_ERROR_PREFIX, test.constants.FILE_INPUT_TOPIC))
        except Exception as e:
            print(test.constants.FILE_INPUT_ERROR_PREFIX, str(e))
            total_errors.append(str(e))
        try:
            total_errors.extend(self.broker_restart_flow())
        except Exception as e:
            print(test.constants.BROKER_RESTART_ERROR_PREFIX, str(e))
            total_errors.append(str(e))
        
        self.assertEqual(total_errors, [])

class DockerSanityTestJVM(DockerSanityTest):
    def setUp(self) -> None:
        self.start_compose(test.constants.JVM_COMPOSE)
    def tearDown(self) -> None:
        self.destroy_compose(test.constants.JVM_COMPOSE)
    def test_bed(self):
        self.execute()

def run_tests(image, mode):
    DockerSanityTest.IMAGE = image

    test_classes_to_run = []
    if mode == "jvm":
        test_classes_to_run = [DockerSanityTestJVM]
    
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    big_suite = unittest.TestSuite(suites_list)
    outfile = open(f"report_{mode}.html", "w")
    runner = HTMLTestRunner.HTMLTestRunner(
                stream=outfile,
                title='Test Report',
                description='This demonstrates the report output.'
                )
    result = runner.run(big_suite)
    return result.failure_count
