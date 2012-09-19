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

/**
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_MessageTest extends PHPUnit_Framework_TestCase
{
	private $test;
	private $encoded;
	private $msg;
	public function setUp() {
		$this->test = 'a sample string';
		$this->encoded = Kafka_Encoder::encode_message($this->test);
		$this->msg = new Kafka_Message($this->encoded);
		
	}
	
	public function testPayload() {
		$this->assertEquals($this->test, $this->msg->payload());
	}
	
	public function testValid() {
		$this->assertTrue($this->msg->isValid());
	}
	
	public function testEncode() {
		$this->assertEquals($this->encoded, $this->msg->encode());
	}
	
	public function testChecksum() {
		$this->assertInternalType('integer', $this->msg->checksum());
	}
	
	public function testSize() {
		$this->assertEquals(strlen($this->test), $this->msg->size());
	}
	
	public function testToString() {
		$this->assertInternalType('string', $this->msg->__toString());
	}
	
	public function testMagic() {
		$this->assertInternalType('integer', $this->msg->magic());
	}
}
