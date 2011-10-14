## LibKafkaConect
## A C++ shared libray for connecting to Kafka
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Warning this is the first time I've made a configure.ac/Makefile.am thing
# Please improve it as I have no idea what I am doing
# @benjamg
#

AC_INIT([LibKafkaConnect], [0.1])
AC_PREREQ([2.59])

AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign -Wall])

AC_PROG_LIBTOOL
AC_PROG_CXX
AC_PROG_CPP

AC_CONFIG_MACRO_DIR([build-aux/m4])

#
# Version number
#
AC_SUBST([KAFKACONNECT_VERSION], [1:0:1])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
