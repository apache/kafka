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
 * Read an entire message set from a stream into an internal buffer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_BoundedByteBuffer_Receive
{
	/**
	 * @var integer
	 */
	protected $size;
	
	/**
	 * @var boolean
	 */
	protected $sizeRead = false;
	
	/**
	 * @var integer
	 */
	protected $remainingBytes = 0;

	/**
	 * @var string resource
	 */
	public $buffer = null;
	
	/**
	 * @var boolean
	 */
	protected $complete = false;
	
	/**
	 *
	 * @var integer
	 */
	protected $maxSize = PHP_INT_MAX;
	
	/**
	 * Constructor
	 *
	 * @param integer $maxSize Max buffer size
	 */
	public function __construct($maxSize = PHP_INT_MAX) {
		$this->maxSize = $maxSize;
	}
	
	/**
	 * Destructor
	 * 
	 * @return void
	 */
	public function __destruct() {
		if (is_resource($this->buffer)) {
			fclose($this->buffer);
		}
	}
	
	/**
	 * Read the request size (4 bytes) if not read yet
	 * 
	 * @param resource $stream Stream resource
	 *
	 * @return integer Number of bytes read
	 * @throws RuntimeException when size is <=0 or >= $maxSize
	 */
	private function readRequestSize($stream) {
		if (!$this->sizeRead) {
			$this->size = fread($stream, 4);
			if ((false === $this->size) || ('' === $this->size)) {
				$errmsg = 'Received nothing when reading from channel, socket has likely been closed.';
				throw new RuntimeException($errmsg);
			}
			$this->size = array_shift(unpack('N', $this->size));
			if ($this->size <= 0 || $this->size > $this->maxSize) {
				throw new RuntimeException($this->size . ' is not a valid message size');
			}
			$this->remainingBytes = $this->size;
			$this->sizeRead = true;
			return 4;
		}
		return 0;
	}
	
	/**
	 * Read a chunk of data from the stream
	 * 
	 * @param resource $stream Stream resource
	 * 
	 * @return integer number of read bytes
	 * @throws RuntimeException when size is <=0 or >= $maxSize
	 */
	public function readFrom($stream) {
		// have we read the request size yet?
		$read = $this->readRequestSize($stream);
		// have we allocated the request buffer yet?
		if (!$this->buffer) {
			$this->buffer = fopen('php://temp', 'w+b');
		}
		// if we have a buffer, read some stuff into it
		if ($this->buffer && !$this->complete) {
			$freadBufferSize = min(8192, $this->remainingBytes);
			if ($freadBufferSize > 0) {
				//TODO: check that fread returns something
				$bytesRead = fwrite($this->buffer, fread($stream, $freadBufferSize));
				$this->remainingBytes -= $bytesRead;
				$read += $bytesRead;
			}
			// did we get everything?
			if ($this->remainingBytes <= 0) {
				rewind($this->buffer);
				$this->complete = true;
			}
		}
		return $read;
	}
	
	/**
	 * Read all the available bytes in the stream
	 * 
	 * @param resource $stream Stream resource
	 * 
	 * @return integer number of read bytes
	 * @throws RuntimeException when size is <=0 or >= $maxSize
	 */
	public function readCompletely($stream) {
		$read = 0;
		while (!$this->complete) {
			$read += $this->readFrom($stream);
		}
		return $read;
	}
}



  