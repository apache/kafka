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

package kafka.common

import util.matching.Regex
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidConfigurationException

trait Config extends Logging {

  def validateChars(prop: String, value: String) {
    val legalChars = "[a-zA-Z0-9\\._\\-]"
    val rgx = new Regex(legalChars + "*")

    rgx.findFirstIn(value) match {
      case Some(t) =>
        if (!t.equals(value))
          throw new InvalidConfigurationException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
      case None => throw new InvalidConfigurationException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
    }
  }
}




