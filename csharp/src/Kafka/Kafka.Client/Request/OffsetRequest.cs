using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class OffsetRequest : AbstractRequest
    {
        /// <summary>
        /// The latest time constant.
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        public OffsetRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="time">The time from which to request offsets.</param>
        /// <param name="maxOffsets">The maximum amount of offsets to return.</param>
        public OffsetRequest(string topic, int partition, long time, int maxOffsets)
        {
            Topic = topic;
            Partition = partition;
            Time = time;
            MaxOffsets = maxOffsets;
        }

        /// <summary>
        /// Gets the time.
        /// </summary>
        public long Time { get; private set; }

        /// <summary>
        /// Gets the maximum number of offsets to return.
        /// </summary>
        public int MaxOffsets { get; private set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic);
        }

        /// <summary>
        /// Converts the request to an array of bytes that is expected by Kafka.
        /// </summary>
        /// <returns>An array of bytes that represents the request.</returns>
        public override byte[] GetBytes()
        {
            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Offsets));
            byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(Topic.Length));
            byte[] topicBytes = Encoding.UTF8.GetBytes(Topic);
            byte[] partitionBytes = BitWorks.GetBytesReversed(Partition);
            byte[] timeBytes = BitWorks.GetBytesReversed(Time);
            byte[] maxOffsetsBytes = BitWorks.GetBytesReversed(MaxOffsets);

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(requestBytes);
            encodedMessageSet.AddRange(topicLengthBytes);
            encodedMessageSet.AddRange(topicBytes);
            encodedMessageSet.AddRange(partitionBytes);
            encodedMessageSet.AddRange(timeBytes);
            encodedMessageSet.AddRange(maxOffsetsBytes);
            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }
    }
}
