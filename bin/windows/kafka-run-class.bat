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

set ivyPath=%USERPROFILE%\.ivy2\cache

set snappy=%ivyPath%/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.5.jar
	call :concat %snappy%

set library=%ivyPath%/org.scala-lang/scala-library/jars/scala-library-2.8.0.jar
	call :concat %library%

set compiler=%ivyPath%/org.scala-lang/scala-compiler/jars/scala-compiler-2.8.0.jar
	call :concat %compiler%

set log4j=%ivyPath%/log4j/log4j/jars/log4j-1.2.15.jar
	call :concat %log4j%

set slf=%ivyPath%/org.slf4j/slf4j-api/jars/slf4j-api-1.6.4.jar
	call :concat %slf%

set zookeeper=%ivyPath%/org.apache.zookeeper/zookeeper/jars/zookeeper-3.3.4.jar
	call :concat %zookeeper%

set jopt=%ivyPath%/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar
	call :concat %jopt%

for %%i in (%BASE_DIR%\core\target\scala-2.8.0\*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\core\lib\*.jar) do (
	call :concat %%i
)

for %%i in (%BASE_DIR%\perf\target\scala-2.8.0/kafka*.jar) do (
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