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
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
import confluent_kafka.admin
import time
import socket
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from HTMLTestRunner import HTMLTestRunner
import constants
import argparse

class DockerSanityTestCommon(unittest.TestCase):
    CONTAINER_NAME="broker"
    IMAGE="apache/kafka"
    
    def resumeImage(self):
        subprocess.run(["docker", "start", self.CONTAINER_NAME])

    def stopImage(self) -> None:
        subprocess.run(["docker", "stop", self.CONTAINER_NAME])

    def startCompose(self, filename) -> None:
        old_string="image: {$IMAGE}"
        new_string=f"image: {self.IMAGE}"

        with open(filename) as f:
            s = f.read()
            if old_string not in s:
                print('"{old_string}" not found in {filename}.'.format(**locals()))

        with open(filename, 'w') as f:
            print('Changing "{old_string}" to "{new_string}" in {filename}'.format(**locals()))
            s = s.replace(old_string, new_string)
            f.write(s)

        subprocess.run(["docker-compose", "-f", filename, "up", "-d"])
        time.sleep(25)
    
    def destroyCompose(self, filename) -> None:
        old_string=f"image: {self.IMAGE}"
        new_string="image: {$IMAGE}"

        subprocess.run(["docker-compose", "-f", filename, "down"])
        time.sleep(10)
        with open(filename) as f:
            s = f.read()
            if old_string not in s:
                print('"{old_string}" not found in {filename}.'.format(**locals()))

        with open(filename, 'w') as f:
            print('Changing "{old_string}" to "{new_string}" in {filename}'.format(**locals()))
            s = s.replace(old_string, new_string)
            f.write(s)

    def create_topic(self, topic):
        kafka_admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": "localhost:9092"})
        new_topic = confluent_kafka.admin.NewTopic(topic, 1, 1)
        kafka_admin.create_topics([new_topic,])
        timeout = constants.CLIENT_TIMEOUT
        while timeout > 0:
            timeout -= 1
            if topic not in kafka_admin.list_topics().topics:
                time.sleep(1)
                continue
            return topic
        return None

    def produce_message(self, topic, producer_config, key, value):
        producer = Producer(producer_config)
        producer.produce(topic, key=key, value=value)
        producer.flush()
        del producer
    
    def consume_message(self, topic, consumer_config):
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        timeout = constants.CLIENT_TIMEOUT
        while timeout > 0:
            message = consumer.poll(1)
            if message is None:
                time.sleep(1)
                timeout -= 1
                continue
            del consumer
            return message
        raise None

    def schema_registry_flow(self):
        print("Running Schema Registry tests")
        errors = []
        schema_registry_conf = {'url': constants.SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        avro_schema = ""
        with open("fixtures/schema.avro") as f:
            avro_schema = f.read()
        avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                     schema_str=avro_schema)
        producer_config = {
            "bootstrap.servers": "localhost:9092",
        }

        avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema)

        key = {"key": "key", "value": ""}
        value = {"value": "message", "key": ""}
        self.produce_message(constants.SCHEMA_REGISTRY_TEST_TOPIC, producer_config, key=avro_serializer(key, SerializationContext(constants.SCHEMA_REGISTRY_TEST_TOPIC, MessageField.KEY)), value=avro_serializer(value, SerializationContext(constants.SCHEMA_REGISTRY_TEST_TOPIC, MessageField.VALUE)))
        time.sleep(3)
        
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            'auto.offset.reset': "earliest"
        }

        message = self.consume_message(constants.SCHEMA_REGISTRY_TEST_TOPIC, consumer_config)

        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.SCHEMA_REGISTRY_ERROR_PREFIX + str(e))
            return
        
        deserialized_value = avro_deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
        deserialized_key = avro_deserializer(message.key(), SerializationContext(message.topic(), MessageField.KEY))
        try:
            self.assertEqual(deserialized_key, key)
        except AssertionError as e:
            errors.append(constants.SCHEMA_REGISTRY_ERROR_PREFIX + str(e))
        try:
            self.assertEqual(deserialized_value, value)
        except AssertionError as e:
            errors.append(constants.SCHEMA_REGISTRY_ERROR_PREFIX + str(e))
        print("Errors in Schema Registry Test Flow:-", errors)
        return errors
    
    def connect_flow(self):
        print("Running Connect tests")
        errors = []
        try:
            self.assertEqual(self.create_topic(constants.CONNECT_TEST_TOPIC), constants.CONNECT_TEST_TOPIC)
        except AssertionError as e:
            errors.append(constants.CONNECT_ERROR_PREFIX + str(e))
            return errors
        subprocess.run(["curl", "-X", "POST", "-H", "Content-Type:application/json", "--data", constants.CONNECT_SOURCE_CONNECTOR_CONFIG, constants.CONNECT_URL])
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            'auto.offset.reset': "earliest"
        }
        message = self.consume_message(constants.CONNECT_TEST_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.CONNECT_ERROR_PREFIX + str(e))
            return errors
        try:
            self.assertIn('User', message.key().decode('ascii'))
        except AssertionError as e:
            errors.append(constants.CONNECT_ERROR_PREFIX + str(e))
        try:
            self.assertIsNotNone(message.value())
        except AssertionError as e:
            errors.append(constants.CONNECT_ERROR_PREFIX + str(e))
        print("Errors in Connect Test Flow:-", errors)
        return errors
    
    def ssl_flow(self):
        print("Running SSL flow tests")
        errors = []
        producer_config = {"bootstrap.servers": "localhost:9093",
                             "security.protocol": "SSL",
                             "ssl.ca.location": constants.SSL_CA_LOCATION,
                             "ssl.certificate.location": constants.SSL_CERTIFICATE_LOCATION,
                             "ssl.key.location": constants.SSL_KEY_LOCATION,
                             "ssl.endpoint.identification.algorithm": "none",
                             "ssl.key.password": constants.SSL_KEY_PASSWORD,
                             'client.id': socket.gethostname() + '2'}

        self.produce_message(constants.SSL_TOPIC, producer_config, "key", "message")

        consumer_config = {
            "bootstrap.servers": "localhost:9093",
            "group.id": "test-group-5",
            'auto.offset.reset': "earliest",
            "security.protocol": "SSL",
            "ssl.ca.location": constants.SSL_CA_LOCATION,
            "ssl.certificate.location": constants.SSL_CERTIFICATE_LOCATION,
            "ssl.key.location": constants.SSL_KEY_LOCATION,
            "ssl.endpoint.identification.algorithm": "none",
            "ssl.key.password": constants.SSL_KEY_PASSWORD
        }
        message = self.consume_message(constants.SSL_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
        try:
            self.assertEqual(message.key(), b'key')
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
        try:
            self.assertEqual(message.value(), b'message')
        except AssertionError as e:
            errors.append(constants.SSL_ERROR_PREFIX + str(e))
        print("Errors in SSL Flow:-", errors)
        return errors
    
    def broker_restart_flow(self):
        print("Running broker restart tests")
        errors = []
        try:
            self.assertEqual(self.create_topic(constants.BROKER_RESTART_TEST_TOPIC), constants.BROKER_RESTART_TEST_TOPIC)
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        
        producer_config = {"bootstrap.servers": "localhost:9092", 'client.id': socket.gethostname()}
        self.produce_message(constants.BROKER_RESTART_TEST_TOPIC, producer_config, "key", "message")

        print("Stopping Image")
        self.stopImage()
        time.sleep(15)

        print("Resuming Image")
        self.resumeImage()
        time.sleep(15)
        consumer_config = {"bootstrap.servers": "localhost:9092", 'group.id': 'test-group-1', 'auto.offset.reset': 'smallest'}
        message = self.consume_message(constants.BROKER_RESTART_TEST_TOPIC, consumer_config)
        try:
            self.assertIsNotNone(message)
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
            return errors
        try:
            self.assertEqual(message.key(), b'key')
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
        try:
            self.assertEqual(message.value(), b'message')
        except AssertionError as e:
            errors.append(constants.BROKER_RESTART_ERROR_PREFIX + str(e))
        print("Errors in Broker Restart Flow:-", errors)
        return errors

    def execute(self):
        total_errors = []
        try:
            total_errors.extend(self.schema_registry_flow())
        except Exception as e:
            print("Schema registry error")
            total_errors.append(str(e))
        try:
            total_errors.extend(self.connect_flow())
        except Exception as e:
            print("Connect flow error")
            total_errors.append(str(e))
        try:
            total_errors.extend(self.ssl_flow())
        except Exception as e:
            print("SSL flow error")
            total_errors.append(str(e))
        try:
            total_errors.extend(self.broker_restart_flow())
        except Exception as e:
            print("Broker restart flow error")
            total_errors.append(str(e))
        
        self.assertEqual(total_errors, [])

class DockerSanityTestKraftMode(DockerSanityTestCommon):
    def setUp(self) -> None:
        self.startCompose("fixtures/kraft/docker-compose.yml")
    def tearDown(self) -> None:
        self.destroyCompose("fixtures/kraft/docker-compose.yml")
    def test_bed(self):
        self.execute()

class DockerSanityTestZookeeper(DockerSanityTestCommon):
    def setUp(self) -> None:
        self.startCompose("fixtures/zookeeper/docker-compose.yml")
    def tearDown(self) -> None:
        self.destroyCompose("fixtures/zookeeper/docker-compose.yml")
    def test_bed(self):
        self.execute()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("mode", default="all")
    args = parser.parse_args()

    DockerSanityTestCommon.IMAGE = args.image

    test_classes_to_run = []
    if args.mode in ("all", "jvm"):
        test_classes_to_run.extend([DockerSanityTestKraftMode, DockerSanityTestZookeeper])
    
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    big_suite = unittest.TestSuite(suites_list)
    outfile = open(f"report.html", "w")
    runner = HTMLTestRunner.HTMLTestRunner(
                stream=outfile,
                title='Test Report',
                description='This demonstrates the report output.'
                )
    runner.run(big_suite)