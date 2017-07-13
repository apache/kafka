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
package kafka.security.auth

import java.util
import java.util.regex.Pattern

import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.utils.CoreUtils.inReadLock
import scala.collection.JavaConverters._

object SimpleRegexAclAuthorizer extends SimpleAclAuthorizer {
  val AuthorizerRegexPrefix = "authorizer.regex.prefix"
}

class SimpleRegexAclAuthorizer extends SimpleAclAuthorizer {

  protected var regexPrefix: String = _
  protected var patternCache = new scala.collection.mutable.HashMap[String, Pattern]

  /**
    * Guaranteed to be called before any authorize call is made.
    */
  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    super.configure(javaConfigs)
    val configs = javaConfigs.asScala
    regexPrefix = configs.getOrElse(SimpleRegexAclAuthorizer.AuthorizerRegexPrefix, "r:").toString
  }

  override def getAcls(resource: Resource): Set[Acl] = {
    val curriedRegexNameMatcher = (t: (Resource, VersionedAcls)) => regexNameMatcher(resource, t._1)
    inReadLock(lock) {
      aclCache.filter(curriedRegexNameMatcher).values.flatMap(_.acls).toSet
    }
  }

  def regexNameMatcher(resource: Resource, r2: Resource): Boolean = {
    resource.name.equals(r2.name) || getPattern(r2.name)
        .map(_.matcher(resource.name).matches())
      .getOrElse(resource.name.equals(r2.name))
  }

  private def getPattern(pattern: String): Option[Pattern] = {
    if (pattern != null && pattern.startsWith(regexPrefix)) {
      val p = pattern.substring(2)
      Some(patternCache.getOrElseUpdate(p, Pattern.compile(p)))
    } else {
      None
    }
  }

}
