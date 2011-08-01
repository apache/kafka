#!/usr/bin/php
<?php

set_include_path(
	implode(PATH_SEPARATOR, array(
		realpath(dirname(__FILE__).'/../lib'),
		get_include_path(),
	))
);
require 'autoloader.php';

$host = 'localhost';
$zkPort  = 2181; //zookeeper
$kPort   = 9092; //kafka server
$topic   = 'test';
$maxSize = 1000000;
$socketTimeout = 5;

$offset    = 0;
$partition = 0;

$consumer = new Kafka_SimpleConsumer($host, $kPort, $socketTimeout, $maxSize);
while (true) {	
	//create a fetch request for topic "test", partition 0, current offset and fetch size of 1MB
	$fetchRequest = new Kafka_FetchRequest($topic, $partition, $offset, $maxSize);
	//get the message set from the consumer and print them out
	$messages = $consumer->fetch($fetchRequest);
	foreach ($messages as $msg) {
		echo "\nconsumed[$offset]: " . $msg->payload();
	}
	//advance the offset after consuming each message
	$offset += $messages->validBytes();
	//echo "\n---[Advancing offset to $offset]------(".date('H:i:s').")";
	unset($fetchRequest);
	sleep(2);
}
