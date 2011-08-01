using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client
{
    /// <summary>
    /// Base request to make to Kafka.
    /// </summary>
    public abstract class AbstractRequest
    {
        /// <summary>
        /// Gets or sets the topic to publish to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Gets or sets the partition to publish to.
        /// </summary>
        public int Partition { get; set; }

        /// <summary>
        /// Converts the request to an array of bytes that is expected by Kafka.
        /// </summary>
        /// <returns>An array of bytes that represents the request.</returns>
        public abstract byte[] GetBytes();

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public abstract bool IsValid();
    }
}
