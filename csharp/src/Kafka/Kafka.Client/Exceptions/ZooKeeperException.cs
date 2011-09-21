namespace Kafka.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public class ZooKeeperException : Exception
    {
        public ZooKeeperException()
        {
        }

        public ZooKeeperException(string message)
            : base(message)
        {
        }

        public ZooKeeperException(string message, Exception exc)
            : base(message, exc)
        {
        }

        protected ZooKeeperException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
