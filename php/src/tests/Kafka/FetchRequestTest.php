<?php

/**
 * Description of FetchRequestTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_FetchRequestTest extends PHPUnit_Framework_TestCase
{
	private $topic;
	private $partition;
	private $offset;
	private $maxSize;
	
	/**
	 * @var Kafka_FetchRequest
	 */
	private $req;

	public function setUp() {
		$this->topic     = 'a test topic';
		$this->partition = 0;
		$this->offset    = 0;
		$this->maxSize   = 10000;
		$this->req = new Kafka_FetchRequest($this->topic, $this->partition, $this->offset, $this->maxSize);
	}
	
	public function testRequestSize() {
		$this->assertEquals(18 + strlen($this->topic) , $this->req->sizeInBytes());
	}
	
	public function testGetters() {
		$this->assertEquals($this->topic,     $this->req->getTopic());
		$this->assertEquals($this->offset,    $this->req->getOffset());
		$this->assertEquals($this->partition, $this->req->getPartition());
	}
	
	public function testWriteTo() {
		$stream = fopen('php://temp', 'w+b');
		$this->req->writeTo($stream);
		rewind($stream);
		$data = stream_get_contents($stream);
		fclose($stream);
		$this->assertEquals(strlen($data), $this->req->sizeInBytes());
		$this->assertContains($this->topic, $data);
		$this->assertContains($this->partition, $data);
	}
	
	public function testWriteToOffset() {
		$this->offset = 14;
		$this->req = new Kafka_FetchRequest($this->topic, $this->partition, $this->offset, $this->maxSize);
		$stream = fopen('php://temp', 'w+b');
		$this->req->writeTo($stream);
		rewind($stream);
		//read it back
		$topicLen = array_shift(unpack('n', fread($stream, 2)));
		$this->assertEquals(strlen($this->topic), $topicLen);
		$this->assertEquals($this->topic,     fread($stream, $topicLen));
		$this->assertEquals($this->partition, array_shift(unpack('N', fread($stream, 4))));
		$int64bit = unpack('N2', fread($stream, 8));
		$this->assertEquals($this->offset,    $int64bit[2]);
		$this->assertEquals($this->maxSize,   array_shift(unpack('N', fread($stream, 4))));
	}
	
	public function testToString() {
		$this->assertContains('topic:'   . $this->topic,     (string)$this->req);
		$this->assertContains('part:'    . $this->partition, (string)$this->req);
		$this->assertContains('offset:'  . $this->offset,    (string)$this->req);
		$this->assertContains('maxSize:' . $this->maxSize,   (string)$this->req);
	}
}