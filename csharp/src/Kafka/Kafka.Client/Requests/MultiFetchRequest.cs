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

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Constructs a multi-consumer request to send to Kafka.
    /// </summary>
    public class MultiFetchRequest : AbstractRequest, IWritable
    {
        public const byte DefaultNumberOfRequestsSize = 2;

        public const byte DefaultHeaderSize =
            DefaultRequestSizeSize + DefaultRequestIdSize + DefaultNumberOfRequestsSize;

        public static int GetRequestLength(IList<FetchRequest> requests, string encoding = DefaultEncoding)
        {
            int requestsLength = 0;
            foreach (var request in requests)
            {
                requestsLength += FetchRequest.GetRequestAsPartOfMultirequestLength(request.Topic, encoding);
            }

            return requestsLength + DefaultHeaderSize;
        }

        /// <summary>
        /// Initializes a new instance of the MultiFetchRequest class.
        /// </summary>
        /// <param name="requests">Requests to package up and batch.</param>
        public MultiFetchRequest(IList<FetchRequest> requests)
        {
            Guard.Assert<ArgumentNullException>(() => requests != null);
            ConsumerRequests = requests;
            int length = GetRequestLength(requests, DefaultEncoding);
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Gets or sets the consumer requests to be batched into this multi-request.
        /// </summary>
        public IList<FetchRequest> ConsumerRequests { get; set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.MultiFetch;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public void WriteTo(MemoryStream output)
        {
            Guard.Assert<ArgumentNullException>(() => output != null);

            using (var writer = new KafkaBinaryWriter(output))
            {
                writer.Write(this.RequestBuffer.Capacity - DefaultRequestSizeSize);
                writer.Write(this.RequestTypeId);
                writer.Write((short)this.ConsumerRequests.Count);
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.Assert<ArgumentNullException>(() => writer != null);

            foreach (var consumerRequest in ConsumerRequests)
            {
                consumerRequest.WriteTo(writer);
            }
        }
    }
}
