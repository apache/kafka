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

/**
  * Mode to control how rack aware replica assignment will be executed
  */
object RackAwareMode {

  /**
    * Ignore all rack information in replica assignment. This is an optional mode used in command line.
    */
  case object Disabled extends RackAwareMode

  /**
    * Assume every broker has rack, or none of the brokers has rack. If only partial brokers have rack, fail fast
    * in replica assignment. This is the default mode in command line tools (TopicCommand and ReassignPartitionsCommand).
    */
  case object Enforced extends RackAwareMode

  /**
    * Use rack information if every broker has a rack. Otherwise, fallback to Disabled mode. This is used in auto topic
    * creation.
    */
  case object Safe extends RackAwareMode
}

sealed trait RackAwareMode
