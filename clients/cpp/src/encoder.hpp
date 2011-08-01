/*
 * encoder.hpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
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
