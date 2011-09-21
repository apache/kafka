using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client.Exceptions
{
    public class ZKRebalancerException : Exception
    {
        public ZKRebalancerException()
        {
        }

        public ZKRebalancerException(string message)
            : base(message)
        {
        }
    }
}
