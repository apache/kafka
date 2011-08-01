using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a multi-consumer request to send to Kafka.
    /// </summary>
    public class MultiFetchRequest : AbstractRequest
    {
        /// <summary>
        /// Initializes a new instance of the MultiFetchRequest class.
        /// </summary>
        /// <param name="requests">Requests to package up and batch.</param>
        public MultiFetchRequest(IList<FetchRequest> requests)
        {
            ConsumerRequests = requests;
        }

        /// <summary>
        /// Gets or sets the consumer requests to be batched into this multi-request.
        /// </summary>
        public IList<FetchRequest> ConsumerRequests { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return ConsumerRequests != null && ConsumerRequests.Count > 0
                && ConsumerRequests.Select(itm => !itm.IsValid()).Count() > 0;
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public override byte[] GetBytes()
        {
            List<byte> messagePack = new List<byte>();
            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.MultiFetch));
            byte[] consumerRequestCountBytes = BitWorks.GetBytesReversed(Convert.ToInt16(ConsumerRequests.Count));

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(requestBytes);
            encodedMessageSet.AddRange(consumerRequestCountBytes);

            foreach (FetchRequest consumerRequest in ConsumerRequests)
            {
                encodedMessageSet.AddRange(consumerRequest.GetInternalBytes());
            }

            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }
    }
}
