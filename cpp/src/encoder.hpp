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
/*
 * encoder.hpp
 */

#ifndef KAFKA_ENCODER_HPP_
#define KAFKA_ENCODER_HPP_

#include <boost/foreach.hpp>
#include "encoder_helper.hpp"

namespace kafkaconnect {

template <typename List>
void encode(std::ostream& stream, const std::string& topic, const uint32_t partition, const List& messages)
{
	// Pre-calculate size of message set
	uint32_t messageset_size = 0;
	BOOST_FOREACH(const std::string& message, messages)
	{
		messageset_size += message_format_header_size + message.length();
	}

	// Packet format is ... packet size (4 bytes)
	encoder_helper::raw(stream, htonl(2 + 2 + topic.size() + 4 + 4 + messageset_size));

	// ... magic number (2 bytes)
	encoder_helper::raw(stream, htons(kafka_format_version));

	// ... topic string size (2 bytes) & topic string
	encoder_helper::raw(stream, htons(topic.size()));
	stream << topic;

	// ... partition (4 bytes)
	encoder_helper::raw(stream, htonl(partition));

	// ... message set size (4 bytes) and message set
	encoder_helper::raw(stream, htonl(messageset_size));
	BOOST_FOREACH(const std::string& message, messages)
	{
		encoder_helper::message(stream, message);
	}
}

}

#endif /* KAFKA_ENCODER_HPP_ */
