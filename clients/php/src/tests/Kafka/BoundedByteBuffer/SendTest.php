<?php

/**
 * Description of Kafka_BoundedByteBuffer_SendTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_BoundedByteBuffer_SendTest extends PHPUnit_Framework_TestCase
{
	private $stream;
	private $topic;
	private $partition;
	private $offset;
	
	/**
	 * @var Kafka_FetchRequest
	 */
	private $req;
	
	/**
	 * @var Kafka_BoundedByteBuffer_Send
	 */
	private $obj = null;

	public function setUp() {
		$this->stream = fopen('php://temp', 'w+b');
		$this->topic     = 'a test topic';
		$this->partition = 0;
		$this->offset    = 0;
		$maxSize         = 10000;
		$this->req = new Kafka_FetchRequest($this->topic, $this->partition, $this->offset, $maxSize);
		$this->obj = new Kafka_BoundedByteBuffer_Send($this->req);
	}

	public function tearDown() {
		fclose($this->stream);
		unset($this->obj);
	}
	
	public function testWriteTo() {
		// 4 bytes = size
		// 2 bytes = request ID
		$this->assertEquals(4 + $this->req->sizeInBytes() + 2, $this->obj->writeTo($this->stream));
	}
	
	public function testWriteCompletely() {
		// 4 bytes = size
		// 2 bytes = request ID
		$this->assertEquals(4 + $this->req->sizeInBytes() + 2, $this->obj->writeCompletely($this->stream));
	}
	
	public function testWriteToWithBigRequest() {
		$topicSize = 9000;
		$this->topic = str_repeat('a', $topicSize); //bigger than the fread buffer, 8192
		$this->req = new Kafka_FetchRequest($this->topic, $this->partition, $this->offset);
		$this->obj = new Kafka_BoundedByteBuffer_Send($this->req);
		// 4 bytes = size
		// 2 bytes = request ID
		//$this->assertEquals(4 + $this->req->sizeInBytes() + 2, $this->obj->writeTo($this->stream));
		$written = $this->obj->writeTo($this->stream);
		$this->assertEquals(4 + 8192, $written);
		$this->assertTrue($written < $topicSize);
	}
	
	public function testWriteCompletelyWithBigRequest() {
		$topicSize = 9000;
		$this->topic = str_repeat('a', $topicSize); //bigger than the fread buffer, 8192
		$this->req = new Kafka_FetchRequest($this->topic, $this->partition, $this->offset);
		$this->obj = new Kafka_BoundedByteBuffer_Send($this->req);
		// 4 bytes = size
		// 2 bytes = request ID
		$this->assertEquals(4 + $this->req->sizeInBytes() + 2, $this->obj->writeCompletely($this->stream));
	}
	
	/**
	 * @expectedException RuntimeException
	 */
	public function testWriteInvalidStream() {
		$this->stream = fopen('php://temp', 'rb'); //read-only mode
		$this->obj->writeTo($this->stream);
		$this->fail('the above call should throw an exception');
	}	
}
