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

namespace Kafka.Client.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;

    internal static class Extensions
    {
        public static string ToMultiString<T>(this IEnumerable<T> items, string separator)
        {
            if (items.Count() == 0)
            {
                return "NULL";
            }

            return String.Join(separator, items);
        }

        public static string ToMultiString<T>(this IEnumerable<T> items, Expression<Func<T, object>> selector, string separator)
        {
            if (items.Count() == 0)
            {
                return "NULL";
            }

            Func<T, object> compiled = selector.Compile();
            return String.Join(separator, items.Select(compiled));
        }
    }
}
