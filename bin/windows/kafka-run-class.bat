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

setlocal enabledelayedexpansion

IF [%1] EQU [] (
	echo USAGE: %0 classname [opts]
	EXIT /B 1
)

rem Using pushd popd to set BASE_DIR to the absolute path
pushd %~dp0..\..
set BASE_DIR=%CD%
popd

IF ["%SCALA_VERSION%"] EQU [""] (
  set SCALA_VERSION=2.13.6
)

IF ["%SCALA_BINARY_VERSION%"] EQU [""] (
  for /f "tokens=1,2 delims=." %%a in ("%SCALA_VERSION%") do (
    set FIRST=%%a
    set SECOND=%%b
    if ["!SECOND!"] EQU [""] (
      set SCALA_BINARY_VERSION=!FIRST!
    ) else (
      set SCALA_BINARY_VERSION=!FIRST!.!SECOND!
    )
  )
)

rem Classpath addition for kafka-core dependencies
for %%i in ("%BASE_DIR%\core\build\dependant-libs-%SCALA_VERSION%\*.jar") do (
	call :concat "%%i"
)

rem Classpath addition for kafka-examples
for %%i in ("%BASE_DIR%\examples\build\libs\kafka-examples*.jar") do (
	call :concat "%%i"
)

rem Classpath addition for kafka-clients
for %%i in ("%BASE_DIR%\clients\build\libs\kafka-clients*.jar") do (
	call :concat "%%i"
)

rem Classpath addition for kafka-streams
for %%i in ("%BASE_DIR%\streams\build\libs\kafka-streams*.jar") do (
	call :concat "%%i"
)

rem Classpath addition for kafka-streams-examples
for %%i in ("%BASE_DIR%\streams\examples\build\libs\kafka-streams-examples*.jar") do (
	call :concat "%%i"
)

for %%i in ("%BASE_DIR%\streams\build\dependant-libs-%SCALA_VERSION%\rocksdb*.jar") do (
	call :concat "%%i"
)

rem Classpath addition for kafka tools
for %%i in ("%BASE_DIR%\tools\build\libs\kafka-tools*.jar") do (
	call :concat "%%i"
)

for %%i in ("%BASE_DIR%\tools\build\dependant-libs-%SCALA_VERSION%\*.jar") do (
	call :concat "%%i"
)

for %%p in (api runtime file json tools) do (
	for %%i in ("%BASE_DIR%\connect\%%p\build\libs\connect-%%p*.jar") do (
		call :concat "%%i"
	)
	if exist "%BASE_DIR%\connect\%%p\build\dependant-libs\*" (
		call :concat "%BASE_DIR%\connect\%%p\build\dependant-libs\*"
	)
)

rem Classpath addition for release
for %%i in ("%BASE_DIR%\libs\*") do (
	call :concat "%%i"
)

rem Classpath addition for core
for %%i in ("%BASE_DIR%\core\build\libs\kafka_%SCALA_BINARY_VERSION%*.jar") do (
	call :concat "%%i"
)

rem JMX settings
IF ["%KAFKA_JMX_OPTS%"] EQU [""] (
	set KAFKA_JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false
)

rem JMX port to use
IF ["%JMX_PORT%"] NEQ [""] (
	set KAFKA_JMX_OPTS=%KAFKA_JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
)

rem Log directory to use
IF ["%LOG_DIR%"] EQU [""] (
    set LOG_DIR=%BASE_DIR%/logs
)

rem Log4j settings
IF ["%KAFKA_LOG4J_OPTS%"] EQU [""] (
	set KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%BASE_DIR%/config/tools-log4j.properties
) ELSE (
  rem create logs directory
  IF not exist "%LOG_DIR%" (
      mkdir "%LOG_DIR%"
  )
)

set KAFKA_LOG4J_OPTS=-Dkafka.logs.dir="%LOG_DIR%" "%KAFKA_LOG4J_OPTS%"

rem Generic jvm settings you want to add
IF ["%KAFKA_OPTS%"] EQU [""] (
	set KAFKA_OPTS=
)

set DEFAULT_JAVA_DEBUG_PORT=5005
set DEFAULT_DEBUG_SUSPEND_FLAG=n
rem Set Debug options if enabled
IF ["%KAFKA_DEBUG%"] NEQ [""] (


	IF ["%JAVA_DEBUG_PORT%"] EQU [""] (
		set JAVA_DEBUG_PORT=%DEFAULT_JAVA_DEBUG_PORT%
	)

	IF ["%DEBUG_SUSPEND_FLAG%"] EQU [""] (
		set DEBUG_SUSPEND_FLAG=%DEFAULT_DEBUG_SUSPEND_FLAG%
	)
	set DEFAULT_JAVA_DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=!DEBUG_SUSPEND_FLAG!,address=!JAVA_DEBUG_PORT!

	IF ["%JAVA_DEBUG_OPTS%"] EQU [""] (
		set JAVA_DEBUG_OPTS=!DEFAULT_JAVA_DEBUG_OPTS!
	)

	echo Enabling Java debug options: !JAVA_DEBUG_OPTS!
	set KAFKA_OPTS=!JAVA_DEBUG_OPTS! !KAFKA_OPTS!
)

rem Which java to use
IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%/bin/java"
)

rem Memory options
IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
	set KAFKA_HEAP_OPTS=-Xmx256M
)

rem JVM performance options
IF ["%KAFKA_JVM_PERFORMANCE_OPTS%"] EQU [""] (
	set KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true
)

IF not defined CLASSPATH (
	echo Classpath is empty. Please build the project first e.g. by running 'gradlew jarAll'
	EXIT /B 2
)

set COMMAND=%JAVA% %KAFKA_HEAP_OPTS% %KAFKA_JVM_PERFORMANCE_OPTS% %KAFKA_JMX_OPTS% %KAFKA_LOG4J_OPTS% -cp %CLASSPATH% %KAFKA_OPTS% %*
rem echo.
rem echo %COMMAND%
rem echo.
%COMMAND%

goto :eof
:concat
IF not defined CLASSPATH (
  set CLASSPATH="%~1"
) ELSE (
  set CLASSPATH=%CLASSPATH%;"%~1"
)
