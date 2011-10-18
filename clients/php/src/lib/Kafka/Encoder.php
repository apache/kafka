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
 * Encode messages and messages sets into the kafka protocol
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
class Kafka_Encoder
{
	/**
	 * 1 byte "magic" identifier to allow format changes
	 * 
	 * @var integer
	 */
	const CURRENT_MAGIC_VALUE = 1;
	
	/**
	 * Encode a message. The format of an N byte message is the following:
     *  - 1 byte: "magic" identifier to allow format changes
     *  - 1 byte:  "compression-attributes" for compression alogrithm
     *  - 4 bytes: CRC32 of the payload
     *  - (N - 5) bytes: payload
	 * 
	 * @param string $msg Message to encode
	 *
	 * @return string
	 */
	static public function encode_message($msg, $compression) {
		// <MAGIC_BYTE: 1 byte> <COMPRESSION: 1 byte> <CRC32: 4 bytes bigendian> <PAYLOAD: N bytes>
		return pack('CCN', self::CURRENT_MAGIC_VALUE, $compression, crc32($msg)) 
			 . $msg;
	}

	/**
	 * Encode a complete request
	 * 
	 * @param string  $topic     Topic
	 * @param integer $partition Partition number
	 * @param array   $messages  Array of messages to send
	 * @param compression $compression flag for type of compression 
	 *
	 * @return string
	 */
	static public function encode_produce_request($topic, $partition, array $messages, $compression) {
		// encode messages as <LEN: int><MESSAGE_BYTES>
		$message_set = '';
		foreach ($messages as $message) {
			$encoded = self::encode_message($message, $compression);
			$message_set .= pack('N', strlen($encoded)) . $encoded;
		}
		// create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
		$data = pack('n', PRODUCE_REQUEST_ID) .
			pack('n', strlen($topic)) . $topic .
			pack('N', $partition) .
			pack('N', strlen($message_set)) . $message_set;
		return pack('N', strlen($data)) . $data;
	}
}
