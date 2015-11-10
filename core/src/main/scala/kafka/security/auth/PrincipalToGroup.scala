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
package kafka.security.auth

import java.security.Principal

import org.apache.kafka.common.Configurable
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
 * A plugin that converts kafka Principal to Groups.
 */
trait PrincipalToGroup extends Configurable {

  /**
   *
   * @param principal
   * @return Set of Groups this principal is part of, empty Set if not part of any group.
   */
  def toGroups(principal: KafkaPrincipal): Set[KafkaPrincipal]
}
