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
    using System.Linq;
    using System.Text;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Constructs a request containing multiple producer requests to send to Kafka.
    /// </summary>
    public class MultiProducerRequest : AbstractRequest, IWritable
    {
        public const byte DefaultRequestsCountSize = 2;

        public static int GetBufferLength(IEnumerable<ProducerRequest> requests)
        {
            Guard.Assert<ArgumentNullException>(() => requests != null);

            return DefaultRequestSizeSize 
                + DefaultRequestIdSize 
                + DefaultRequestsCountSize
                + (int)requests.Sum(x => x.RequestBuffer.Length - DefaultRequestIdSize - DefaultRequestSizeSize);
        }

        /// <summary>
        /// Initializes a new instance of the MultiProducerRequest class.
        /// </summary>
        /// <param name="requests">
        /// The list of individual producer requests to send in this request.
        /// </param>
        public MultiProducerRequest(IEnumerable<ProducerRequest> requests)
        {
            Guard.Assert<ArgumentNullException>(() => requests != null);
            int length = GetBufferLength(requests);
            ProducerRequests = requests;
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Gets or sets the list of producer requests to be sent in batch.
        /// </summary>
        public IEnumerable<ProducerRequest> ProducerRequests { get; set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.MultiProduce;
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

            writer.Write((short)this.ProducerRequests.Count());
            foreach (var request in ProducerRequests)
            {
                request.WriteTo(writer);
            }
        }

        public override string ToString()
        {
            using (var reader = new KafkaBinaryReader(this.RequestBuffer))
            {
                return ParseFrom(reader, (int)this.RequestBuffer.Length);
            }
        }

        public static string ParseFrom(KafkaBinaryReader reader, int count)
        {
            Guard.Assert<ArgumentNullException>(() => reader != null);

            var sb = new StringBuilder();
            sb.Append("Request size: ");
            sb.Append(reader.ReadInt32());
            sb.Append(", RequestId: ");
            short reqId = reader.ReadInt16();
            sb.Append(reqId);
            sb.Append("(");
            sb.Append((RequestTypes)reqId);
            sb.Append("), Single Requests: {");
            int i = 1;
            while (reader.BaseStream.Position != reader.BaseStream.Length)
            {
                sb.Append("Request ");
                sb.Append(i);
                sb.Append(" {");
                int msgSize = 0;
                sb.Append(ProducerRequest.ParseFrom(reader, msgSize));
                sb.AppendLine("} ");
                i++;
            }

            return sb.ToString();
        }
    }
}
