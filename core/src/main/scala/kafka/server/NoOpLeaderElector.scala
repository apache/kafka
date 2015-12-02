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
package kafka.server

import kafka.utils.Logging

class NoOpLeaderElector extends LeaderElector with Logging {
  override def startup: Unit = {
    trace("Broker is started in non-controller mode - leader elector will take no actions upon startup")
  }

  override def elect: Boolean = {
    trace("Broker is started in non-controller mode - leader elector will take no actions upon election")
    false
  }

  override def close: Unit = {
    trace("Broker is started in non-controller mode - leader elector will take no actions upon closing")
  }

  override def amILeader: Boolean = {
    trace("Broker is started in non-controller mode")
    false
  }

  override def resign(): Unit = {
    trace("Broker is started in non-controller mode - it cannot resign from leadership")
  }
}