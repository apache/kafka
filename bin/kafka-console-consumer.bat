@echo off
SetLocal
set KAFKA_OPTS=-Xmx512M -server -Dcom.sun.management.jmxremote -Dlog4j.configuration=file:"%CD%\kafka-console-consumer-log4j.properties"
kafka-run-class.bat kafka.consumer.ConsoleConsumer %*
EndLocal
