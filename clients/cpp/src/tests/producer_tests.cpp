/*
 * producer_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafkaconnect
#include <boost/test/unit_test.hpp>

#include <memory>

#include <boost/thread.hpp>

#include "../producer.hpp"

BOOST_AUTO_TEST_CASE(basic_message_test)
{
	boost::asio::io_service io_service;
	std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::asio::ip::tcp::acceptor acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	kafkaconnect::producer producer(io_service);
	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	boost::asio::ip::tcp::socket socket(io_service);
	acceptor.accept(socket);

	while(!producer.is_connected())
	{
		boost::this_thread::sleep(boost::posix_time::seconds(1));
	}

	std::vector<std::string> messages;
	messages.push_back("so long and thanks for all the fish");
	producer.send(messages, "mice", 42);

	boost::array<char, 1024> buffer;
	boost::system::error_code error;
	size_t len = socket.read_some(boost::asio::buffer(buffer), error);

	BOOST_CHECK_EQUAL(len, 4 + 2 + 2 + strlen("mice") + 4 + 4 + 9 + strlen("so long and thanks for all the fish"));
	BOOST_CHECK_EQUAL(buffer[3], 2 + 2 + strlen("mice") + 4 + 4 + 9 + strlen("so long and thanks for all the fish"));
	BOOST_CHECK_EQUAL(buffer[5], 0);
	BOOST_CHECK_EQUAL(buffer[7], strlen("mice"));
	BOOST_CHECK_EQUAL(buffer[8], 'm');
	BOOST_CHECK_EQUAL(buffer[8 + strlen("mice") - 1], 'e');
	BOOST_CHECK_EQUAL(buffer[11 + strlen("mice")], 42);
	BOOST_CHECK_EQUAL(buffer[15 + strlen("mice")], 9 + strlen("so long and thanks for all the fish"));
	BOOST_CHECK_EQUAL(buffer[16 + strlen("mice")], 0);
	BOOST_CHECK_EQUAL(buffer[25 + strlen("mice")], 's');

	work.reset();
	io_service.stop();
}

