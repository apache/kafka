/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class SecurityUtils {

    public static final String WILDCARD_MARKER = "*";

    public static KafkaPrincipal parseKafkaPrincipal(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        String[] split = str.split(":", 2);

        if (split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        return new KafkaPrincipal(split[0], split[1]);
    }

    /**
     * Returns true if two strings match, both of which might be ending in a special wildcard which matches everything.
     * Examples:
     *   matchWildcardSuffixedString("rob", "rob") => true
     *   matchWildcardSuffixedString("*", "rob") => true
     *   matchWildcardSuffixedString("ro*", "rob") => true
     *   matchWildcardSuffixedString("rob", "bob") => false
     *   matchWildcardSuffixedString("ro*", "bob") => false
     *
     *   matchWildcardSuffixedString("rob", "*") => true
     *   matchWildcardSuffixedString("rob", "ro*") => true
     *   matchWildcardSuffixedString("bob", "ro*") => false
     *
     *   matchWildcardSuffixedString("ro*", "ro*") => true
     *   matchWildcardSuffixedString("rob*", "ro*") => false
     *   matchWildcardSuffixedString("ro*", "rob*") => true
     *
     * @param valueInZk Value stored in ZK in either resource name or Acl.
     * @param input Value present in the request.
     * @return true if there is a match (including wildcard-suffix matching).
     */
    public static boolean matchWildcardSuffixedString(String valueInZk, String input) {

        if (valueInZk.equals(input) || valueInZk.equals(WILDCARD_MARKER) || input.equals(WILDCARD_MARKER)) {
            // if strings are equal or either of acl or input is a wildcard
            return true;

        } else if (valueInZk.endsWith(WILDCARD_MARKER)) {

            String aclPrefix = valueInZk.substring(0, valueInZk.length() - WILDCARD_MARKER.length());
            if (input.endsWith(WILDCARD_MARKER)) {
                // when both acl and input ends with wildcard, non-wildcard prefix of input should start with non-wildcard prefix of acl
                String inputPrefix = input.substring(0, input.length() - WILDCARD_MARKER.length());
                return inputPrefix.startsWith(aclPrefix);
            } else {
                // when acl ends with wildcard but input doesn't, then input should start with non-wildcard prefix of acl
                return input.startsWith(aclPrefix);
            }

        } else {

            if (input.endsWith(WILDCARD_MARKER)) {
                // when input ends with wildcard but acl doesn't, then acl should start with non-wildcard prefix of input
                String inputPrefix = input.substring(0, input.length() - WILDCARD_MARKER.length());
                return valueInZk.startsWith(inputPrefix);
            } else {
                // when neither acl nor input ends with wildcard, they have to match exactly.
                return valueInZk.equals(input);
            }

        }
    }
    //  /**
//    * Returns true if two strings match, both of which might be ending in a special wildcard which matches everything.
//    * Examples:
//    *   matchWildcardSuffixedString("rob", "rob") => true
//    *   matchWildcardSuffixedString("*", "rob") => true
//    *   matchWildcardSuffixedString("ro*", "rob") => true
//    *   matchWildcardSuffixedString("rob", "bob") => false
//    *   matchWildcardSuffixedString("ro*", "bob") => false
//    *
//    *   matchWildcardSuffixedString("rob", "*") => true
//    *   matchWildcardSuffixedString("rob", "ro*") => true
//    *   matchWildcardSuffixedString("bob", "ro*") => false
//    *
//    *   matchWildcardSuffixedString("ro*", "ro*") => true
//    *   matchWildcardSuffixedString("rob*", "ro*") => false
//    *   matchWildcardSuffixedString("ro*", "rob*") => true
//    *
//    * @param valueInZk Value stored in ZK in either resource name or Acl.
//    * @param input Value present in the request.
//    * @return true if there is a match (including wildcard-suffix matching).
//    */
//  def matchWildcardSuffixedString(valueInZk: String, input: String): Boolean = {
//
//    if (valueInZk.equals(input) || valueInZk.equals(Acl.WildCardString) || input.equals(Acl.WildCardString)) {
//      // if strings are equal or either of acl or input is a wildcard
//      true
//
//    } else if (valueInZk.endsWith(Acl.WildCardString)) {
//
//      val aclPrefix = valueInZk.substring(0, valueInZk.length - Acl.WildCardString.length)
//      if (input.endsWith(Acl.WildCardString)) {
//        // when both acl and input ends with wildcard, non-wildcard prefix of input should start with non-wildcard prefix of acl
//        val inputPrefix = input.substring(0, input.length - Acl.WildCardString.length)
//        inputPrefix.startsWith(aclPrefix)
//      } else {
//        // when acl ends with wildcard but input doesn't, then input should start with non-wildcard prefix of acl
//        input.startsWith(aclPrefix)
//      }
//
//    } else {
//
//      if (input.endsWith(Acl.WildCardString)) {
//        // when input ends with wildcard but acl doesn't, then acl should start with non-wildcard prefix of input
//        val inputPrefix = input.substring(0, input.length - Acl.WildCardString.length)
//        valueInZk.startsWith(inputPrefix)
//      } else {
//        // when neither acl nor input ends with wildcard, they have to match exactly.
//        valueInZk.equals(input)
//      }
//
//    }
//  }

}
