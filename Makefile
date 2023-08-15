##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
SCALA_VERSION := $(shell grep -o -E '^scalaVersion=[0-9]+\.[0-9]+\.[0-9]+' gradle.properties | cut -c14-)
SCALA_MAJOR_VERSION := $(shell grep -o -E '^scalaVersion=[0-9]+\.[0-9]+' gradle.properties | cut -c14-)
KAFKA_VERSION := $(shell grep -o -E '^version=[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?' gradle.properties | cut -c9-)
IMAGE_NAME=aivenoy/kafka

BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
IMAGE_TAG := $(subst /,_,$(BRANCH))

.PHONY: all build clean

all: clean build

clean:
	./gradlew clean

build: core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz

core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz:
	echo "Version: $(KAFKA_VERSION)-$(SCALA_VERSION)"
	./gradlew -PscalaVersion=$(SCALA_VERSION) testJar releaseTarGz

.PHONY: docker_image
docker_image: build
	docker build . \
		--build-arg _SCALA_VERSION=$(SCALA_MAJOR_VERSION) \
		--build-arg _KAFKA_VERSION=$(KAFKA_VERSION) \
		-t $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

# TODO publish docker images
