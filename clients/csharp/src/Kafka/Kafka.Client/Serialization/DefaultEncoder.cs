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

namespace Kafka.Client.Serialization
{
    using Kafka.Client.Messages;

    /// <summary>
    /// Default serializer that expects <see cref="Message" /> object
    /// </summary>
    public class DefaultEncoder : IEncoder<Message>
    {
        /// <summary>
        /// Do nothing with data
        /// </summary>
        /// <param name="data">
        /// The data, that are already in <see cref="Message" /> format.
        /// </param>
        /// <returns>
        /// Serialized data
        /// </returns>
        public Message ToMessage(Message data)
        {
            return data;
        }
    }
}
