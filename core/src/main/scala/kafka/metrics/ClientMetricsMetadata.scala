/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.metrics

import kafka.metrics.ClientMetricsConfig.ClientMatchingParams._
import kafka.network.RequestChannel
import org.apache.kafka.common.errors.InvalidConfigurationException

import java.util.regex.{Pattern, PatternSyntaxException}
import scala.collection.mutable

/**
 * Information from the client's metadata is gathered from the client's request.
 */
object ClientMetricsMetadata {
  def apply(request: RequestChannel.Request, clientInstanceId: String): ClientMetricsMetadata = {
    val instance = new ClientMetricsMetadata
    val ctx = request.context
    val clientSoftwareName = if (ctx.clientInformation != null) ctx.clientInformation.softwareName() else ""
    val clientSoftwareVersion = if (ctx.clientInformation != null) ctx.clientInformation.softwareVersion() else ""
    instance.init(clientInstanceId, ctx.clientId(), clientSoftwareName, clientSoftwareVersion,
                  ctx.clientAddress.getHostAddress, ctx.clientAddress.getHostAddress) // TODO: Fix Port
    instance
  }

  def apply(clientInstanceId: String, clientId: String, clientSoftwareName: String,
            clientSoftwareVersion: String, clientSourceAddress: String, clientSourcePort: String): ClientMetricsMetadata = {
    val instance = new ClientMetricsMetadata
    instance.init(clientInstanceId, clientId, clientSoftwareName, clientSoftwareVersion, clientSourceAddress, clientSourcePort)
    instance
  }

  /**
   * Parses the client matching patterns and builds a map with entries that has
   * (PatternName, PatternValue) as the entries.
   *  Ex: "VERSION=1.2.3" would be converted to a map entry of (Version, 1.2.3)
   *
   *  NOTES:
   *  1. Client match pattern splits the input into two parts separated by first
   *     occurrence of the character '='
   *  2. '*' is considered as invalid client match pattern
   * @param patterns List of client matching pattern strings
   * @return map of client matching pattern entries
   */
  def parseMatchingPatterns(patterns: List[String]) : Map[String, String] = {
    val patternsMap = mutable.Map[String, String]()
    if (patterns != null) {
      patterns.foreach(x => {
        val nameValuePair = x.split("=", 2).map(x => x.trim)
        if (nameValuePair.size == 2 && isValidParam(nameValuePair(0)) && validRegExPattern(nameValuePair(1))) {
          patternsMap += (nameValuePair(0) -> nameValuePair(1))
        } else {
          throw new InvalidConfigurationException("Illegal client matching pattern: " + x)
        }
      })
    }
    patternsMap.toMap
  }

  private def validRegExPattern(inputPattern :String): Boolean = {
    try {
      Pattern.compile(inputPattern)
      true
    } catch {
      case _: PatternSyntaxException =>
        false
    }
  }

}

class ClientMetricsMetadata {
  var attributesMap: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

  private def init(clientInstanceId: String,
                   clientId: String,
                   clientSoftwareName: String,
                   clientSoftwareVersion: String,
                   clientSourceAddress: String,
                   clientSourcePort: String): Unit = {
    attributesMap(CLIENT_INSTANCE_ID) = clientInstanceId
    attributesMap(CLIENT_ID) = clientId
    attributesMap(CLIENT_SOFTWARE_NAME) = clientSoftwareName
    attributesMap(CLIENT_SOFTWARE_VERSION) = clientSoftwareVersion
    attributesMap(CLIENT_SOURCE_ADDRESS) = clientSourceAddress
    attributesMap(CLIENT_SOURCE_PORT) = clientSourcePort
  }
  def getClientId: Option[String] = attributesMap.get(CLIENT_ID)

  def isMatched(patterns: Option[Map[String, String]]) : Boolean = {
    // Empty pattern or missing pattern still considered as a match
    patterns match {
      case Some(patterns) if patterns.nonEmpty => matchPatterns(patterns)
      case _ => true
    }
  }

  private def matchPatterns(matchingPatterns: Map[String, String]) : Boolean = {
    matchingPatterns.forall {
      case (k, v) =>
        val attribute = attributesMap.getOrElse(k, null)
        attribute != null && v.r.anchored.findAllMatchIn(attribute).nonEmpty
    }
  }
}
