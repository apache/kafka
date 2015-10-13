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
package kafka.utils

import java.io.File
import joptsimple.OptionParser
import java.net.URI
import java.security.URIParameter
import javax.security.auth.login.Configuration

import kafka.common.KafkaException

object ToolsUtils {

  def isSecure(loginConfigFile: String): Boolean = {
    var isSecurityEnabled = false
    if (loginConfigFile != null && loginConfigFile.length > 0) {
      val configFile: File = new File(loginConfigFile)
      if (!configFile.canRead) {
        throw new KafkaException(s"File $loginConfigFile cannot be read.")
      }
      try {
        val configUri: URI = configFile.toURI
        val loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configUri))
        isSecurityEnabled = loginConf.getAppConfigurationEntry("Client") != null
      } catch {
        case ex: Exception => {
          throw new KafkaException(ex)
        }
      }
    }
    isSecurityEnabled
  }
  
  def validatePortOrDie(parser: OptionParser, hostPort: String) = {
    val hostPorts: Array[String] = if(hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
    val validHostPort = hostPorts.filter {
      hostPortData =>
        org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null
    }
    val isValid = !(validHostPort.isEmpty) && validHostPort.size == hostPorts.length
    if(!isValid)
      CommandLineUtils.printUsageAndDie(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")
  }
}
