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
 * Send a request to Kafka
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_BoundedByteBuffer_Send
{
	/**
	 * @var integer
	 */
	protected $size;
	
	/**
	 * @var boolean
	 */
	protected $sizeWritten = false; 

	/**
	 * @var string resource
	 */
	protected $buffer;
	
	/**
	 * @var boolean
	 */
	protected $complete = false;
	
	/**
	 * Constructor
	 * 
	 * @param Kafka_FetchRequest $req Request object
	 */
	public function __construct(Kafka_FetchRequest $req) {
		$this->size = $req->sizeInBytes() + 2;
		$this->buffer = fopen('php://temp', 'w+b');
		fwrite($this->buffer, pack('n', $req->id));
		$req->writeTo($this->buffer);
		rewind($this->buffer);
		//fseek($this->buffer, $req->getOffset(), SEEK_SET);
	}
	
	/**
	 * Try to write the request size if we haven't already
	 * 
	 * @param resource $stream Stream resource
	 *
	 * @return integer Number of bytes read
	 * @throws RuntimeException when size is <=0 or >= $maxSize
	 */
	private function writeRequestSize($stream) {
		if (!$this->sizeWritten) {
			if (!fwrite($stream, pack('N', $this->size))) {
				throw new RuntimeException('Cannot write request to stream (' . error_get_last() . ')');
			}
			$this->sizeWritten = true;
			return 4;
		}
		return 0;
	}
	
	/**
	 * Write a chunk of data to the stream
	 * 
	 * @param resource $stream Stream resource
	 * 
	 * @return integer number of written bytes
	 * @throws RuntimeException
	 */
	public function writeTo($stream) {
		// have we written the request size yet?
		$written = $this->writeRequestSize($stream);
		
		// try to write the actual buffer itself
		if ($this->sizeWritten && !feof($this->buffer)) {
			//TODO: check that fread returns something
			$written += fwrite($stream, fread($this->buffer, 8192));
		}
		// if we are done, mark it off
		if (feof($this->buffer)) {
			$this->complete = true;
			fclose($this->buffer);
		}
		return $written;
	}
	
	/**
	 * Write the entire request to the stream
	 * 
	 * @param resource $stream Stream resource
	 * 
	 * @return integer number of written bytes
	 */
	public function writeCompletely($stream) {
		$written = 0;
		while (!$this->complete) {
			$written += $this->writeTo($stream);
		}
		//echo "\nWritten " . $written . ' bytes ';
		return $written;
	}
}
