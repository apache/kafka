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
 * Simple Kafka Producer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Producer
{
	/**
	 * @var integer
	 */
	protected $request_key;

	/**
	 * @var resource
	 */
	protected $conn;
	
	/**
	 * @var string
	 */
	protected $host;
	
	/**
	 * @var integer
	 */
	protected $port;

	/**
	 * @var integer
	 */
	protected $compression;

	/**
	 * Constructor
	 * 
	 * @param integer $host Host 
	 * @param integer $port Port
	 */
	public function __construct($host, $port) {
		$this->request_key = 0;
		$this->host = $host;
		$this->port = $port;
		$this->compression = 0;
	}
	
	/**
	 * Connect to Kafka via a socket
	 * 
	 * @return void
	 * @throws RuntimeException
	 */
	public function connect() {
		if (!is_resource($this->conn)) {
			$this->conn = stream_socket_client('tcp://' . $this->host . ':' . $this->port, $errno, $errstr);
		}
		if (!is_resource($this->conn)) {
			throw new RuntimeException('Cannot connect to Kafka: ' . $errstr, $errno);
		}
	}

	/**
	 * Close the socket
	 * 
	 * @return void
	 */
	public function close() {
		if (is_resource($this->conn)) {
			fclose($this->conn);
		}
	}

	/**
	 * Send messages to Kafka
	 * 
	 * @param array   $messages  Messages to send
	 * @param string  $topic     Topic
	 * @param integer $partition Partition
	 *
	 * @return boolean
	 */
	public function send(array $messages, $topic, $partition = 0xFFFFFFFF) {
		$this->connect();
		return fwrite($this->conn, Kafka_Encoder::encode_produce_request($topic, $partition, $messages, $this->compression));
	}

	/**
	 * When serializing, close the socket and save the connection parameters
	 * so it can connect again
	 * 
	 * @return array Properties to save
	 */
	public function __sleep() {
		$this->close();
		return array('request_key', 'host', 'port');
	}

	/**
	 * Restore parameters on unserialize
	 * 
	 * @return void
	 */
	public function __wakeup() {
		
	}
}
