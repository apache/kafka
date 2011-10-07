#!/usr/bin/php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

<?php

set_include_path(
	implode(PATH_SEPARATOR, array(
		realpath(dirname(__FILE__).'/../lib'),
		get_include_path(),
	))
);
require 'autoloader.php';

define('PRODUCE_REQUEST_ID', 0);


$host = 'localhost';
$port = 9092;
$topic = 'test';

$producer = new Kafka_Producer($host, $port);
$in = fopen('php://stdin', 'r');
while (true) {
	echo "\nEnter comma separated messages:\n";
	$messages = explode(',', fgets($in));
	foreach (array_keys($messages) as $k) {
		//$messages[$k] = trim($messages[$k]);
	}
	$bytes = $producer->send($messages, $topic);
	printf("\nSuccessfully sent %d messages (%d bytes)\n\n", count($messages), $bytes);
}
