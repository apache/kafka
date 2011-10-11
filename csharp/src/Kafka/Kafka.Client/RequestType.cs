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

namespace Kafka.Client
{
    /// <summary>
    /// Requests types for Kafka
    /// </summary>
    /// <remarks>
    /// Many of these are not in play yet.
    /// </remarks>
    public enum RequestType
    {
        /// <summary>
        /// Produce a message.
        /// </summary>
        Produce = 0,

        /// <summary>
        /// Fetch a message.
        /// </summary>
        Fetch = 1,

        /// <summary>
        /// Multi-fetch messages.
        /// </summary>
        MultiFetch = 2,
        
        /// <summary>
        /// Multi-produce messages.
        /// </summary>
        MultiProduce = 3,

        /// <summary>
        /// Gets offsets.
        /// </summary>
        Offsets = 4
    }
}
