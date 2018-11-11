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
package kafka.admin

import java.util.Properties
import kafka.utils.ZkUtils

@deprecated("This class is deprecated since AdminUtilities will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
class TestAdminUtils extends AdminUtilities {
  override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configs: Properties): Unit = {}
  override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {new Properties}
  override def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configs: Properties): Unit = {}
  override def changeUserOrUserClientIdConfig(zkUtils: ZkUtils, sanitizedEntityName: String, configs: Properties): Unit = {}
  override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties): Unit = {}
}
