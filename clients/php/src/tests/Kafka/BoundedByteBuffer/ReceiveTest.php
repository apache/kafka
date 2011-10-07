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
 * Description of Kafka_BoundedByteBuffer_ReceiveTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_BoundedByteBuffer_ReceiveTest extends PHPUnit_Framework_TestCase
{
	private $stream = null;
	private $size1  = 0;
	private $msg1   = '';
	private $size2  = 0;
	private $msg2   = '';
	
	/**
	 * @var Kafka_BoundedByteBuffer_Receive
	 */
	private $obj = null;
	
	/**
	 * Append two message sets to a sample stream to verify that only the first one is read
	 */
	public function setUp() {
		$this->stream = fopen('php://temp', 'w+b');
		$this->msg1 = 'test message';
		$this->msg2 = 'another message';
		$this->size1 = strlen($this->msg1);
		$this->size2 = strlen($this->msg2);
		fwrite($this->stream, pack('N', $this->size1));
		fwrite($this->stream, $this->msg1);
		fwrite($this->stream, pack('N', $this->size2));
		fwrite($this->stream, $this->msg2);
		rewind($this->stream);
		$this->obj = new Kafka_BoundedByteBuffer_Receive;
	}

	public function tearDown() {
		fclose($this->stream);
		unset($this->obj);
	}
	
	public function testReadFrom() {
		$this->assertEquals($this->size1 + 4, $this->obj->readFrom($this->stream));
		$this->assertEquals($this->msg1, stream_get_contents($this->obj->buffer));
		//test that we don't go beyond the first message set
		$this->assertEquals(0, $this->obj->readFrom($this->stream));
		$this->assertEquals($this->size1 + 4, ftell($this->stream));
	}
	
	public function testReadCompletely() {
		$this->assertEquals($this->size1 + 4, $this->obj->readCompletely($this->stream));
		$this->assertEquals($this->msg1, stream_get_contents($this->obj->buffer));
		//test that we don't go beyond the first message set
		$this->assertEquals(0, $this->obj->readCompletely($this->stream));
		$this->assertEquals($this->size1 + 4, ftell($this->stream));
	}
	
	public function testReadFromOffset() {
		fseek($this->stream, $this->size1 + 4);
		$this->obj = new Kafka_BoundedByteBuffer_Receive;
		$this->assertEquals($this->size2 + 4, $this->obj->readFrom($this->stream));
		$this->assertEquals($this->msg2, stream_get_contents($this->obj->buffer));
		//test that we reached the end of the stream (2nd message set)
		$this->assertEquals(0, $this->obj->readFrom($this->stream));
		$this->assertEquals($this->size1 + 4 + $this->size2 + 4, ftell($this->stream));
	}
	
	public function testReadCompletelyOffset() {
		fseek($this->stream, $this->size1 + 4);
		$this->obj = new Kafka_BoundedByteBuffer_Receive;
		$this->assertEquals($this->size2 + 4, $this->obj->readCompletely($this->stream));
		$this->assertEquals($this->msg2, stream_get_contents($this->obj->buffer));
		//test that we reached the end of the stream (2nd message set)
		$this->assertEquals(0, $this->obj->readCompletely($this->stream));
		$this->assertEquals($this->size1 + 4 + $this->size2 + 4, ftell($this->stream));
	}
	
	/**
	 * @expectedException RuntimeException
	 */
	public function testInvalidStream() {
		$this->stream = fopen('php://temp', 'w+b');
		$this->obj->readFrom($this->stream);
		$this->fail('The above call should throw an exception');	
	}
	
	/**
	 * @expectedException RuntimeException
	 */
	public function testInvalidSizeTooBig() {
		$maxSize = 10;
		$this->obj = new Kafka_BoundedByteBuffer_Receive($maxSize);
		$this->stream = fopen('php://temp', 'w+b');
		fwrite($this->stream, pack('N', $maxSize + 1));
		fwrite($this->stream, $this->msg1);
		rewind($this->stream);
		$this->obj->readFrom($this->stream);
		$this->fail('The above call should throw an exception');
	}
	
	/**
	 * @expectedException RuntimeException
	 */
	public function testInvalidSizeNotPositive() {
		$this->stream = fopen('php://temp', 'w+b');
		fwrite($this->stream, pack('N', 0));
		fwrite($this->stream, '');
		rewind($this->stream);
		$this->obj->readFrom($this->stream);
		$this->fail('The above call should throw an exception');
	}
}
