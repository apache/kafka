# C++ kafka library
This library allows you to produce messages to the Kafka distributed publish/subscribe messaging service.

## Requirements
Tested on Ubuntu and Redhat both with g++ 4.4 and Boost 1.46.1

## Installation
Make sure you have g++ and the latest version of Boost: 
http://gcc.gnu.org/
http://www.boost.org/

```bash
./autoconf.sh
./configure
```

Run this to generate the makefile for your system. Do this first.


```bash
make
```

builds the producer example and the KafkaConnect library


```bash
make check
```

builds and runs the unit tests, 


```bash
make install
```

to install as a shared library to 'default' locations (/usr/local/lib and /usr/local/include on linux) 


## Usage
Example.cpp is a very basic Kafka Producer


## API docs
There isn't much code, if I get around to writing the other parts of the library I'll document it sensibly, 
for now have a look at the header file:  /src/producer.hpp


## Contact for questions

Ben Gray, MediaSift Ltd.

http://twitter.com/benjamg


