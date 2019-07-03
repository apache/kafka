/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util
import java.util.Properties

import scala.collection.JavaConverters._

/**
  * In order to have these implicits in scope, add the following import:
  *
  * `import kafka.utils.Implicits._`
  */
object Implicits {

  /**
   * The java.util.Properties.putAll override introduced in Java 9 is seen as an overload by the
   * Scala compiler causing ambiguity errors in some cases. The `++=` methods introduced via
   * implicits provide a concise alternative.
   *
   * See https://github.com/scala/bug/issues/10418 for more details.
   */
  implicit class PropertiesOps(properties: Properties) {

    def ++=(props: Properties): Unit =
      (properties: util.Hashtable[AnyRef, AnyRef]).putAll(props)

    def ++=(map: collection.Map[String, AnyRef]): Unit =
      (properties: util.Hashtable[AnyRef, AnyRef]).putAll(map.asJava)

  }

}
