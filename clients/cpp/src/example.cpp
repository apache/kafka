
#include <exception>
#include <cstdlib>
#include <iostream>
#include <string>

#include <boost/thread.hpp>

#include "producer.hpp"

int main(int argc, char* argv[])
{
	std::string hostname = (argc >= 2) ? argv[1] : "localhost";
	std::string port = (argc >= 3) ? argv[2] : "9092";

	boost::asio::io_service io_service;
	std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	kafkaconnect::producer producer(io_service);
	producer.connect(hostname, port);

	while(!producer.is_connected())
	{
		boost::this_thread::sleep(boost::posix_time::seconds(1));
	}

	std::vector<std::string> messages;
	messages.push_back("So long and thanks for all the fish");
	messages.push_back("Time is an illusion. Lunchtime doubly so.");
	producer.send(messages, "test");

	work.reset();
	io_service.stop();

	return EXIT_SUCCESS;
}

