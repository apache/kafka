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
 * Represents a request object
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_FetchRequest extends Kafka_Request
{
	/**
	 * @var string
	 */
	private $topic;
	
	/**
	 * @var integer
	 */
	private $partition;
	
	/**
	 * @var integer
	 */
	private $offset;
	
	/**
	 * @var integer
	 */
	private $maxSize;
	
	/**
	 * @param string  $topic     Topic
	 * @param integer $partition Partition
	 * @param integer $offset    Offset
	 * @param integer $maxSize   Max buffer size
	 */
	public function __construct($topic, $partition = 0, $offset = 0, $maxSize = 1000000) {
		$this->id        = Kafka_RequestKeys::FETCH;
		$this->topic     = $topic;
		$this->partition = $partition;
		$this->offset    = $offset;
		$this->maxSize   = $maxSize;
	}
	
	/**
	 * Write the request to the output stream
	 * 
	 * @param resource $stream Output stream
	 * 
	 * @return void
	 */
	public function writeTo($stream) {
		//echo "\nWriting request to stream: " . (string)$this;
		// <topic size: short> <topic: bytes>
		fwrite($stream, pack('n', strlen($this->topic)) . $this->topic);
		// <partition: int> <offset: Long> <maxSize: int>
		fwrite($stream, pack('N', $this->partition));
		
//TODO: need to store a 64bit integer (bigendian), but PHP only supports 32bit integers: 
//setting first 32 bits to 0
		fwrite($stream, pack('N2', 0, $this->offset));
		fwrite($stream, pack('N', $this->maxSize));
		//echo "\nWritten request to stream: " .(string)$this;
	}
	
	/**
	 * Get request size in bytes
	 * 
	 * @return integer
	 */
	public function sizeInBytes() {
		return 2 + strlen($this->topic) + 4 + 8 + 4;
	}
	
	/**
	 * Get current offset
	 *
	 * @return integer
	 */
	public function getOffset() {
		return $this->offset;
	}
	
	/**
	 * Get topic
	 * 
	 * @return string
	 */
	public function getTopic() {
		return $this->topic;
	}
	
	/**
	 * Get partition
	 * 
	 * @return integer
	 */
	public function getPartition() {
		return $this->partition;
	}
	
	/**
	 * String representation of the Fetch Request
	 * 
	 * @return string
	 */
	public function __toString()
	{
		return 'topic:' . $this->topic . ', part:' . $this->partition . ' offset:' . $this->offset . ' maxSize:' . $this->maxSize;
	}
}

