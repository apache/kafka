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
 * A sequence of messages stored in a byte buffer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_MessageSet implements Iterator
{	
	/**
	 * @var integer
	 */
	protected $validByteCount = 0;
	
	/**
	 * @var boolean
	 */
	private $valid = false;
	
	/**
	 * @var array
	 */
	private $array = array();
	
	/**
	 * Constructor
	 * 
	 * @param resource $stream    Stream resource
	 * @param integer  $errorCode Error code
	 */
	public function __construct($stream, $errorCode = 0) {
		$data = stream_get_contents($stream);
		$len = strlen($data);
		$ptr = 0;
		while ($ptr <= ($len - 4)) {
			$size = array_shift(unpack('N', substr($data, $ptr, 4)));
			$ptr += 4;
			$this->array[] = new Kafka_Message(substr($data, $ptr, $size));
			$ptr += $size;
			$this->validByteCount += 4 + $size;
		}
		fclose($stream);
	}
	
	/**
	 * Get message set size in bytes
	 * 
	 * @return integer
	 */
	public function validBytes() {
		return $this->validByteCount;
	}
	
	/**
	 * Get message set size in bytes
	 * 
	 * @return integer
	 */
	public function sizeInBytes() {
		return $this->validBytes();
	}
	
	/**
	 * next
	 * 
	 * @return void
	 */
	public function next() {
		$this->valid = (FALSE !== next($this->array)); 
	}	
	
	/**
	 * valid
	 * 
	 * @return boolean
	 */
	public function valid() {
		return $this->valid;
	}
	
	/**
	 * key
	 * 
	 * @return integer
	 */
	public function key() {
		return key($this->array); 
	}
	
	/**
	 * current
	 * 
	 * @return Kafka_Message 
	 */
	public function current() {
		return current($this->array);
	}
	
	/**
	 * rewind
	 * 
	 * @return void
	 */
	public function rewind() {
		$this->valid = (FALSE !== reset($this->array)); 
	}
}
