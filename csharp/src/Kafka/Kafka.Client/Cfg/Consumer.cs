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

    public class Consumer : ConfigurationElement
    {
        [ConfigurationProperty("numberOfTries")]
        public short NumberOfTries
        {
            get
            {
                return (short)this["numberOfTries"];
            }

            set
            {
                this["numberOfTries"] = value;
            }
        }

        [ConfigurationProperty("groupId")]
        public string GroupId
        {
            get
            {
                return (string)this["groupId"];
            }

            set
            {
                this["groupId"] = value;
            }
        }

        [ConfigurationProperty("timeout")]
        public int Timeout
        {
            get
            {
                return (int)this["timeout"];
            }

            set
            {
                this["timeout"] = value;
            }
        }

        [ConfigurationProperty("autoOffsetReset")]
        public string AutoOffsetReset
        {
            get
            {
                return (string)this["autoOffsetReset"];
            }

            set
            {
                this["autoOffsetReset"] = value;
            }
        }

        [ConfigurationProperty("autoCommit")]
        public bool AutoCommit
        {
            get
            {
                return (bool)this["autoCommit"];
            }

            set
            {
                this["autoCommit"] = value;
            }
        }

        [ConfigurationProperty("autoCommitIntervalMs")]
        public int AutoCommitIntervalMs
        {
            get
            {
                return (int)this["autoCommitIntervalMs"];
            }

            set
            {
                this["autoCommitIntervalMs"] = value;
            }
        }

        [ConfigurationProperty("fetchSize")]
        public int FetchSize
        {
            get
            {
                return (int)this["fetchSize"];
            }

            set
            {
                this["fetchSize"] = value;
            }
        }

        [ConfigurationProperty("backOffIncrementMs")]
        public int BackOffIncrementMs
        {
            get
            {
                return (int)this["backOffIncrementMs"];
            }

            set
            {
                this["backOffIncrementMs"] = value;
            }
        }
    }
}
