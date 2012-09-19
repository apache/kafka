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

namespace Kafka.Client.Utils
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.RegularExpressions;

    internal static class Guard
    {
        public static void NotNull(object parameter, string paramName)
        {
            if (parameter == null)
            {
                throw new ArgumentNullException(paramName);
            }
        }

        public static void Count(ICollection parameter, int length, string paramName)
        {
            if (parameter.Count != length)
            {
                throw new ArgumentOutOfRangeException(paramName, parameter.Count, string.Empty);
            }
        }

        public static void Greater(int parameter, int expected, string paramName)
        {
            if (parameter <= expected)
            {
                throw new ArgumentOutOfRangeException(paramName, parameter, string.Empty);
            }
        }

        public static void NotNullNorEmpty(string parameter, string paramName)
        {
            if (string.IsNullOrEmpty(parameter))
            {
                throw new ArgumentException("Given string is empty", paramName);
            }
        }

        public static void AllNotNull(IEnumerable parameter, string paramName)
        {
            foreach (var par in parameter)
            {
                if (par == null)
                {
                    throw new ArgumentNullException(paramName);
                }
            }
        }

        /// <summary>
        /// Checks whether given expression is true. Throws given exception type if not.
        /// </summary>
        /// <typeparam name="TException">
        /// Type of exception that i thrown when condition is not met.
        /// </typeparam>
        /// <param name="assertion">
        /// The assertion.
        /// </param>
        public static void Assert<TException>(Expression<Func<bool>> assertion)
            where TException : Exception, new()
        {
            var compiled = assertion.Compile();
            var evaluatedValue = compiled();
            if (!evaluatedValue)
            {
                var e = (Exception)Activator.CreateInstance(
                    typeof(TException),
                    new object[] { string.Format("'{0}' is not met.", Normalize(assertion.ToString())) });
                throw e;
            }
        }

        /// <summary>
        /// Creates string representation of lambda expression with unnecessary information 
        /// stripped out. 
        /// </summary>
        /// <param name="expression">Lambda expression to process. </param>
        /// <returns>Normalized string representation. </returns>
        private static string Normalize(string expression)
        {
            var result = expression;
            var replacements = new Dictionary<Regex, string>()
            {
                { new Regex("value\\([^)]*\\)\\."), string.Empty },
                { new Regex("\\(\\)\\."), string.Empty },
                { new Regex("\\(\\)\\ =>"), string.Empty },                
                { new Regex("Not"), "!" }            
            };

            foreach (var pattern in replacements)
            {
                result = pattern.Key.Replace(result, pattern.Value);
            }

            result = result.Trim();
            return result;
        }
    }
}
