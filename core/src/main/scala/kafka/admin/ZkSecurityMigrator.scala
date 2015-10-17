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

package kafka.admin

import javax.security.auth.login.LoginException
import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ToolsUtils, Logging, ZKGroupTopicDirs, ZkUtils, CommandLineUtils}
import org.apache.kafka.common.security.JaasUtils

object ZkSecurityMigrator extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser()

    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").
      withRequiredArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
                                  "takes a comma-separated list of host:port pairs.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val zkConnectionTimeoutOpt = parser.accepts("connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if(options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, "ZooKeeper Migration Tool Help")

    if(!options.has(jaasFileOpt) ||
      !JaasUtils.isSecure(options.valueOf(jaasFileOpt))) {
      error("No JAAS configuration file has been found. Please make sure that "
            + "you have set the option --jaas.file correctly and that the file"
            + " is valid")
      System.exit(1) 
    }

    val jaasFile = options.valueOf(jaasFileOpt)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt)
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt)

    val zkUtils = ZkUtils.apply(zkUrl, zkSessionTimeout, zkConnectionTimeout, true)
    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
    }
  }
}