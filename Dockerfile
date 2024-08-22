FROM ubuntu:bionic as base-builder

ENV ANURAG=test
ENV GOPATH=$HOME/go
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

RUN apt-get update \
    && apt-get install -y curl git \
    && apt-get install -y build-essential \
    && apt-get install -y cmake \
    && apt-get install -y wget \
    && apt-get install -y unzip \
    && apt-get install -y libssl-dev \
    && apt-get install -y zlib1g-dev \
    && apt-get clean

FROM base-builder as base-builder-extended
RUN curl -sL https://deb.nodesource.com/setup_14.x

FROM base-builder as golang
RUN curl -O https://storage.googleapis.com/golang/go1.15.2.linux-amd64.tar.gz && touch go

FROM base-builder as source-code
RUN git clone https://github.com/prometheus/prometheus.git prometheus/

FROM base-builder-extended as builder
COPY --from=golang go /usr/local
COPY --from=source-code prometheus/ prometheus/
RUN cd prometheus/ && echo "Change" > change.txt

# Additional time-consuming commands
RUN cd /tmp \
    && wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tgz \
    && tar -xzf Python-3.8.0.tgz \
    && cd Python-3.8.0 \
    && ./configure \
    && make \
    && make install

FROM ubuntu:bionic as final

COPY --from=builder prometheus/change.txt change.txt
