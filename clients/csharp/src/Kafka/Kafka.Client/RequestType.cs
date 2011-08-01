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
