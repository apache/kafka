/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
    public class ZKConfig
    {
        public ZKConfig()
            : this(null, 6000, 6000, 2000)
        {
        }

        public ZKConfig(string zkconnect, int zksessionTimeoutMs, int zkconnectionTimeoutMs, int zksyncTimeMs)
        {
            this.ZkConnect = zkconnect;
            this.ZkConnectionTimeoutMs = zkconnectionTimeoutMs;
            this.ZkSessionTimeoutMs = zksessionTimeoutMs;
            this.ZkSyncTimeMs = zksyncTimeMs;
        }

        public string ZkConnect { get; set; }

        public int ZkSessionTimeoutMs { get; set; }

        public int ZkConnectionTimeoutMs { get; set; }

        public int ZkSyncTimeMs { get; set; }
    }
}
