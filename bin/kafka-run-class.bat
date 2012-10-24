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
	echo "USAGE: $0 classname [opts]"
	goto :eof
)

set BASE_DIR=%CD%\..
set CLASSPATH=
echo %BASE_DIR%

for %%i in (%BASE_DIR%\project\boot\scala-2.8.0\lib\*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\core\target\scala_2.8.0\*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\core\lib\*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\perf\target\scala_2.8.0/kafka*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\core\lib_managed\scala_2.8.0\compile\*.jar) do (
	call :concat %%i
)

IF ["%KAFKA_JMX_OPTS%"] EQU [""] (
	set KAFKA_JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
)

IF ["%KAFKA_OPTS%"] EQU [""] (
	set KAFKA_OPTS=-Xmx512M -server -Dlog4j.configuration=file:"%BASE_DIR%\config\log4j.properties"
)

IF ["%JMX_PORT%"] NEQ [""] (
	set KAFKA_JMX_OPTS=%KAFKA_JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
)

IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%/bin/java"
)

set SEARCHTEXT=\bin\..
set REPLACETEXT=
set CLASSPATH=!CLASSPATH:%SEARCHTEXT%=%REPLACETEXT%!
set COMMAND= %JAVA% %KAFKA_OPTS% %KAFKA_JMX_OPTS% -cp %CLASSPATH% %*
set SEARCHTEXT=-cp ;
set REPLACETEXT=-cp 
set COMMAND=!COMMAND:%SEARCHTEXT%=%REPLACETEXT%!

%COMMAND%

:concat
set CLASSPATH=%CLASSPATH%;"%1"