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
