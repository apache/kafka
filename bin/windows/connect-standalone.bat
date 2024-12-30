@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

IF [%1] EQU [] (
	echo USAGE: %0 connect-standalone.properties
	EXIT /B 1
)

SetLocal
rem Using pushd popd to set BASE_DIR to the absolute path
pushd %~dp0..\..
set BASE_DIR=%CD%
popd

rem Log4j settings
IF ["%KAFKA_LOG4J_OPTS%"] EQU [""] (
        set KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=%BASE_DIR%/config/connect-log4j2.yaml
) ELSE (
        echo %KAFKA_LOG4J_OPTS% | findstr /r /c:"log4j\.[^ ]*$" >nul
        IF %ERRORLEVEL% == 0 (
            echo DEPRECATED: A Log4j 1.x configuration file has been detected, which is no longer recommended.
            echo To use a Log4j 2.x configuration, please see https://logging.apache.org/log4j/2.x/migrate-from-log4j1.html#Log4j2ConfigurationFormat for details about Log4j configuration file migration.
            echo You can also use the %BASE_DIR%/config/connect-log4j2.yaml file as a starting point. Make sure to remove the Log4j 1.x configuration after completing the migration.
        )
)

"%~dp0kafka-run-class.bat" org.apache.kafka.connect.cli.ConnectStandalone %*
EndLocal
