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

function test_autoload($className)
{
	$classFile = str_replace('_', DIRECTORY_SEPARATOR, $className) . '.php';
	if (function_exists('stream_resolve_include_path')) {
		$file = stream_resolve_include_path($classFile);
	} else {
		foreach (explode(PATH_SEPARATOR, get_include_path()) as $path) {
			if (file_exists($path . '/' . $classFile)) {
				$file = $path . '/' . $classFile;
				break;
			}
		}
	}
	/* If file is found, store it into the cache, classname <-> file association */
	if (($file !== false) && ($file !== null)) {
		include $file;
		return;
	}

	throw new RuntimeException($className. ' not found');
}

// register the autoloader
spl_autoload_register('test_autoload');

set_include_path(
	implode(PATH_SEPARATOR, array(
		realpath(dirname(__FILE__).'/../lib'),
		get_include_path(),
	))
);

date_default_timezone_set('Europe/London');
 