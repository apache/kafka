/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

<?php
if (!defined('PRODUCE_REQUEST_ID')) {
	define('PRODUCE_REQUEST_ID', 0);
}

/**
 * Description of EncoderTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_EncoderTest extends PHPUnit_Framework_TestCase
{
	public function testEncodedMessageLength() {
		$test = 'a sample string';
		$encoded = Kafka_Encoder::encode_message($test);
		$this->assertEquals(5 + strlen($test), strlen($encoded));
	}
	
	public function testByteArrayContainsString() {
		$test = 'a sample string';
		$encoded = Kafka_Encoder::encode_message($test);
		$this->assertContains($test, $encoded);
	}
	
	public function testEncodedMessages() {
		$topic     = 'sample topic';
		$partition = 1;
		$messages  = array(
			'test 1',
			'test 2 abcde',
		);
		$encoded = Kafka_Encoder::encode_produce_request($topic, $partition, $messages);
		$this->assertContains($topic, $encoded);
		$this->assertContains($partition, $encoded);
		foreach ($messages as $msg) {
			$this->assertContains($msg, $encoded);
		}
		$size = 4 + 2 + 2 + strlen($topic) + 4 + 4;
		foreach ($messages as $msg) {
			$size += 9 + strlen($msg);
		}
		$this->assertEquals($size, strlen($encoded));
	}
}
