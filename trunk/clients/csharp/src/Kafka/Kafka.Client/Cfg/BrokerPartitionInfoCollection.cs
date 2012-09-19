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

namespace Kafka.Client.Cfg
{
    using System.Configuration;

    public class BrokerPartitionInfoCollection : ConfigurationElementCollection
    {
        public BrokerPartitionInfo this[int index]
        {
            get
            {
                return this.BaseGet(index) as BrokerPartitionInfo;
            }

            set
            {
                if (this.BaseGet(index) != null)
                {
                    this.BaseRemoveAt(index);
                }

                this.BaseAdd(index, value);
            }
        }
    
        protected override ConfigurationElement CreateNewElement()
        {
            return new BrokerPartitionInfo();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((BrokerPartitionInfo)element).Id;
        }
    }
}