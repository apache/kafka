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
 