include $(GOROOT)/src/Make.inc

TARG=kafka
GOFILES=\
	src/kafka.go\
	src/message.go\
	src/converts.go\
	src/consumer.go\
	src/publisher.go\
	src/timing.go\
	src/request.go\

include $(GOROOT)/src/Make.pkg

tools: force
	make -C tools/consumer clean all
	make -C tools/publisher clean all
	make -C tools/offsets clean all

format:
	gofmt -w -tabwidth=2 -tabindent=false src/*.go tools/consumer/*.go  tools/publisher/*.go kafka_test.go

full: format clean install tools

.PHONY: force 
