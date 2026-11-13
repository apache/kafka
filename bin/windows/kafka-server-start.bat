@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

IF [%1] EQU [] (
	echo USAGE: %0 server.properties
	EXIT /B 1
)

SetLocal
IF ["%KAFKA_LOG4J_OPTS%"] EQU [""] (
    set KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=%~dp0../../config/log4j2.yaml
)
IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
    rem detect OS architecture
    IF /I "%PROCESSOR_ARCHITECTURE%" EQU "x86" (
        IF NOT DEFINED PROCESSOR_ARCHITEW6432 (
            rem 32-bit OS
            set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
        ) ELSE (
            rem 32-bit process running on 64-bit Windows system
            set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
        )
    ) ELSE (
        rem 64-bit OS
        set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
    )
)
"%~dp0kafka-run-class.bat" kafka.Kafka %*
EndLocal
