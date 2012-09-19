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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request containing multiple producer requests to send to Kafka.
    /// </summary>
    public class MultiProducerRequest : AbstractRequest
    {
        /// <summary>
        /// Initializes a new instance of the MultiProducerRequest class.
        /// </summary>
        public MultiProducerRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiProducerRequest class.
        /// </summary>
        /// <param name="producerRequests">
        /// The list of individual producer requests to send in this request.
        /// </param>
        public MultiProducerRequest(IList<ProducerRequest> producerRequests)
        {
            ProducerRequests = producerRequests;
        }

        /// <summary>
        /// Gets or sets the list of producer requests to be sent in batch.
        /// </summary>
        public IList<ProducerRequest> ProducerRequests { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return ProducerRequests != null && ProducerRequests.Count > 0
                && ProducerRequests.Select(itm => !itm.IsValid()).Count() > 0;
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public override byte[] GetBytes()
        {
            List<byte> messagePack = new List<byte>();
            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.MultiProduce));
            byte[] producerRequestCountBytes = BitWorks.GetBytesReversed(Convert.ToInt16(ProducerRequests.Count));

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(requestBytes);
            encodedMessageSet.AddRange(producerRequestCountBytes);

            foreach (ProducerRequest producerRequest in ProducerRequests)
            {
                encodedMessageSet.AddRange(producerRequest.GetInternalBytes());
            }

            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }
    }
}
