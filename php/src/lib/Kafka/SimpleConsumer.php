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
 * Simple Kafka Consumer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_SimpleConsumer
{
	/**
	 * @var string
	 */
	protected $host             = 'localhost';
	
	/**
	 * @var integer
	 */
	protected $port             = 9092;
	
	/**
	 * @var integer
	 */
	protected $socketTimeout    = 10;
	
	/**
	 * @var integer
	 */
	protected $socketBufferSize = 1000000;

	/**
	 * @var resource
	 */
	protected $conn = null;
	
	/**
	 * Constructor
	 * 
	 * @param integer $host             Kafka Hostname
	 * @param integer $port             Port
	 * @param integer $socketTimeout    Socket timeout
	 * @param integer $socketBufferSize Socket max buffer size
	 */
	public function __construct($host, $port, $socketTimeout, $socketBufferSize) {
		$this->host = $host;
		$this->port = $port;
		$this->socketTimeout    = $socketTimeout;
		$this->socketBufferSize = $socketBufferSize;
	}
	
	/**
	 * Connect to Kafka via socket
	 * 
	 * @return void
	 */
	public function connect() {
		if (!is_resource($this->conn)) {
			$this->conn = stream_socket_client('tcp://' . $this->host . ':' . $this->port, $errno, $errstr);
			if (!$this->conn) {
				throw new RuntimeException($errstr, $errno);
			}
			stream_set_timeout($this->conn,      $this->socketTimeout);
			stream_set_read_buffer($this->conn,  $this->socketBufferSize);
			stream_set_write_buffer($this->conn, $this->socketBufferSize);
			//echo "\nConnected to ".$this->host.":".$this->port."\n";
		}
	}

	/**
	 * Close the connection
	 * 
	 * @return void
	 */
	public function close() {
		if (is_resource($this->conn)) {
			fclose($this->conn);
		}
	}

	/**
	 * Send a request and fetch the response
	 * 
	 * @param Kafka_FetchRequest $req Request
	 *
	 * @return Kafka_MessageSet $messages
	 */
	public function fetch(Kafka_FetchRequest $req) {
		$this->connect();
		$this->sendRequest($req);
		//echo "\nRequest sent: ".(string)$req."\n";
		$response = $this->getResponse();
		//var_dump($response);
		$this->close();
		return new Kafka_MessageSet($response['response']->buffer, $response['errorCode']);
	}
	
	/**
	 * Send the request
	 * 
	 * @param Kafka_FetchRequest $req Request
	 * 
	 * @return void
	 */
	protected function sendRequest(Kafka_FetchRequest $req) {
		$send = new Kafka_BoundedByteBuffer_Send($req);
		$send->writeCompletely($this->conn);
	}
	
	/**
	 * Get the response
	 * 
	 * @return array
	 */
	protected function getResponse() {
		$response = new Kafka_BoundedByteBuffer_Receive();
		$response->readCompletely($this->conn);
		
		rewind($response->buffer);
		// this has the side effect of setting the initial position of buffer correctly
		$errorCode = array_shift(unpack('n', fread($response->buffer, 2))); 
		//rewind($response->buffer);
		return array(
			'response'  => $response, 
			'errorCode' => $errorCode,
		);
	}
	
}
