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
import constants
import argparse

class DockerSanityTest(unittest.TestCase):
    IMAGE="apache/kafka"
    
    def resumeImage(self):
        subprocess.run(["docker", "start", constants.BROKER_CONTAINER])

    def stopImage(self) -> None:
        subprocess.run(["docker", "stop", constants.BROKER_CONTAINER])

    def startCompose(self, filename) -> None:
        old_string="image: {$IMAGE}"
        new_string=f"image: {self.IMAGE}"
        with open(filename) as f:
            s = f.read()
        with open(filename, 'w') as f:
            s = s.replace(old_string, new_string)
            f.write(s)

        subprocess.run(["docker-compose", "-f", filename, "up", "-d"])
    
    def destroyCompose(self, filename) -> None:
        old_string=f"image: {self.IMAGE}"
        new_string="image: {$IMAGE}"
        subprocess.run(["docker-compose", "-f", filename, "down"])
        with open(filename) as f:
            s = f.read()
        with open(filename, 'w') as f:
            s = s.replace(old_string, new_string)
            f.write(s)

    def create_topic(self, topic, topic_config):
        command = [constants.KAFKA_TOPICS, "--create", "--topic", topic]
        command.extend(topic_config)
        subprocess.run(command)
        check_command = [constants.KAFKA_TOPICS, "--list"]
        check_command.extend(topic_config)
        output = subprocess.check_output(check_command, timeout=constants.CLIENT_TIMEOUT)
        if topic in output.decode("utf-8"):
            return True
        return False
        
    def produce_message(self, topic, producer_config, key, value):
        command = ["echo", f'"{key}:{value}"', "|", constants.KAFKA_CONSOLE_PRODUCER, "--topic", topic, "--property", "'parse.key=true'", "--property", "'key.separator=:'"]
        command.extend(producer_config)
        subprocess.run(["bash", "-c", " ".join(command)], timeout=constants.CLIENT_TIMEOUT)
    
    def consume_message(self, topic, consumer_config):
        command = [constants.KAFKA_CONSOLE_CONSUMER, "--topic", topic, "--property", "'print.key=true'", "--property", "'key.separator=:'", "--from-beginning", "--max-messages", "1"]
        command.extend(consumer_config)
        message = subprocess.check_output(["bash", "-c", " ".join(command)], timeout=constants.CLIENT_TIMEOUT)
        return message.decode("utf-8").strip()

    def ssl_flow(self):
        print(f"Running {constants.SSL_FLOW_TESTS}")
        errors = []
        try:
            self.assertTrue(self.create_topic(constants.SSL_TOPIC, ["--bootstrap-server", "localhost:9093", "--command-config", constants.SSL_CLIENT_CONFIG]))
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
            return errors

        producer_config = ["--bootstrap-server", "localhost:9093",
                           "--producer.config", constants.SSL_CLIENT_CONFIG]
        self.produce_message(constants.SSL_TOPIC, producer_config, "key", "message")

        consumer_config = [
            "--bootstrap-server", "localhost:9093",
            "--property", "auto.offset.reset=earliest",
            "--consumer.config", constants.SSL_CLIENT_CONFIG,
        ]
        message = self.consume_message(constants.SSL_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
        try:
            self.assertEqual(message, "key:message")
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
        if errors:
            print(f"Errors in {constants.SSL_FLOW_TESTS}:- {errors}")
        else:
            print(f"No errors in {constants.SSL_FLOW_TESTS}")
        return errors
    
    def broker_restart_flow(self):
        print(f"Running {constants.BROKER_RESTART_TESTS}")
        errors = []
        
        try:
            self.assertTrue(self.create_topic(constants.BROKER_RESTART_TEST_TOPIC, ["--bootstrap-server", "localhost:9092"]))
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        
        producer_config = ["--bootstrap-server", "localhost:9092", "--property", "client.id=host"]
        self.produce_message(constants.BROKER_RESTART_TEST_TOPIC, producer_config, "key", "message")

        print("Stopping Container")
        self.stopImage()
        print("Resuming Image")
        self.resumeImage()

        consumer_config = ["--bootstrap-server", "localhost:9092", "--property", "auto.offset.reset=earliest"]
        message = self.consume_message(constants.BROKER_RESTART_TEST_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        try:
            self.assertEqual(message, "key:message")
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
        if errors:
            print(f"Errors in {constants.BROKER_RESTART_TESTS}:- {errors}")
        else:
            print(f"No errors in {constants.BROKER_RESTART_TESTS}")
        return errors

    def execute(self):
        total_errors = []
        try:
            total_errors.extend(self.ssl_flow())
        except Exception as e:
            print("SSL flow error", str(e))
            total_errors.append(str(e))
        try:
            total_errors.extend(self.broker_restart_flow())
        except Exception as e:
            print("Broker restart flow error", str(e))
            total_errors.append(str(e))
        
        self.assertEqual(total_errors, [])

class DockerSanityTestKraftMode(DockerSanityTest):
    def setUp(self) -> None:
        self.startCompose(constants.KRAFT_COMPOSE)
    def tearDown(self) -> None:
        self.destroyCompose(constants.KRAFT_COMPOSE)
    def test_bed(self):
        self.execute()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("mode", default="all")
    args = parser.parse_args()

    DockerSanityTest.IMAGE = args.image

    test_classes_to_run = []
    if args.mode == "jvm":
        test_classes_to_run = [DockerSanityTestKraftMode]
    
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    big_suite = unittest.TestSuite(suites_list)
    outfile = open(f"report_{args.mode}.html", "w")
    runner = HTMLTestRunner.HTMLTestRunner(
                stream=outfile,
                title='Test Report',
                description='This demonstrates the report output.'
                )
    runner.run(big_suite)