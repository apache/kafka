<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2011 Lorenzo Alberton
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @version   $Revision: $
 * @link      http://sna-projects.com/kafka/
 */

/**
 * A message. The format of an N byte message is the following:
 * 1 byte "magic" identifier to allow format changes
 * 4 byte CRC32 of the payload
 * N - 5 byte payload
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Message
{
	/*
	private $currentMagicValue = Kafka_Encoder::CURRENT_MAGIC_VALUE;
	private $magicOffset   = 0;
	private $magicLength   = 1;
	private $crcOffset     = 1; // MagicOffset + MagicLength
	private $crcLength     = 4;
	private $payloadOffset = 5; // CrcOffset + CrcLength
	private $headerSize    = 5; // PayloadOffset
	*/
	
	/**
	 * @var string
	 */
	private $payload = null;
	
	/**
	 * @var integer
	 */
	private $size    = 0;
	
	/**
	 * @var string
	 */
	private $crc     = false;
	
	/**
	 * Constructor
	 * 
	 * @param string $data Message payload
	 */
	public function __construct($data) {
		$this->payload = substr($data, 5);
		$this->crc     = crc32($this->payload);
		$this->size    = strlen($this->payload);
	}
	
	/**
	 * Encode a message
	 * 
	 * @return string
	 */
	public function encode() {
		return Kafka_Encoder::encode_message($this->payload);
	}
	
	/**
	 * Get the message size
	 * 
	 * @return integer
	 */
	public function size() {
		return $this->size;
	}
  
	/**
	 * Get the magic value
	 * 
	 * @return integer
	 */
	public function magic() {
		return Kafka_Encoder::CURRENT_MAGIC_VALUE;
	}
	
	/**
	 * Get the message checksum
	 * 
	 * @return integer
	 */
	public function checksum() {
		return $this->crc;
	}
	
	/**
	 * Get the message payload
	 * 
	 * @return string
	 */
	public function payload() {
		return $this->payload;
	}
	
	/**
	 * Verify the message against the checksum
	 * 
	 * @return boolean
	 */
	public function isValid() {
		return ($this->crc === crc32($this->payload));
	}
  
	/**
	 * Debug message
	 * 
	 * @return string
	 */
	public function __toString() {
		return 'message(magic = ' . Kafka_Encoder::CURRENT_MAGIC_VALUE . ', crc = ' . $this->crc .
			', payload = ' . $this->payload . ')';
	}
}
