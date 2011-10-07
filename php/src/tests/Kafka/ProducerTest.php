<?php

/**
 * Override connect() method of base class
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_ProducerMock extends Kafka_Producer {
	public function connect() {
		if (!is_resource($this->conn)) {
			$this->conn = fopen('php://temp', 'w+b');
		}
	}
	
	public function getData() {
		$this->connect();
		rewind($this->conn);
		return stream_get_contents($this->conn);
	}
}

/**
 * Description of ProducerTest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_ProducerTest extends PHPUnit_Framework_TestCase
{
	/**
	 * @var Kafka_Producer
	 */
	private $producer;
	
	public function setUp() {
		$this->producer = new Kafka_ProducerMock('localhost', 1234);
	}
	
	public function tearDown() {
		$this->producer->close();
		unset($this->producer);
	}


	public function testProducer() {
		$messages = array(
			'test 1',
			'test 2 abc',
		);
		$topic = 'a topic';
		$partition = 3;
		$this->producer->send($messages, $topic, $partition);
		$sent = $this->producer->getData();
		$this->assertContains($topic, $sent);
		$this->assertContains($partition, $sent);
		foreach ($messages as $msg) {
			$this->assertContains($msg, $sent);
		}
	}
}
