# Profiling Kafka #

See here: http://sna-projects.com/kafka/performance.php

## Getting Started Locally ##

1. Build Kafka itself, start servers etc.
2. ./sbt 
   project perf
   package-all
3. Make sure report-html/data (or whichever dir you want to dump simulator data to) exists.
4. ./run-simulator.sh

## Getting Started With Remote Tests ##

1. Look at util-bin/remote-kafka-env.sh and the constants there.
2. Scripts assume that you have kafka built and installed on the
remote hosts.
3. Example: `./util-bin/run-fetchsize-test.sh user@host user@host 1 dir/report-html/fetch_size_test`.
4.  Start a web-server or copy the results somewhere you can view with
a browser.You can view the results of a specific test run by setting
`report.html?dataDir=my_test`.

