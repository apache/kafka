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

namespace Kafka.Client.Producers.Async
{
    using Kafka.Client.Requests;

    /// <summary>
    /// Callback made when a message request is finished being sent asynchronously.
    /// </summary>
    /// <typeparam name="T">
    /// Must be of type <see cref="AbstractRequest"/> and represents the type of message 
    /// sent to Kafka.
    /// </typeparam>
    /// <param name="request">The request that was sent to the server.</param>
    public delegate void MessageSent<T>(RequestContext<T> request) where T : AbstractRequest;
}
