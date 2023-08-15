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
SCALA_VERSION=2.13
KAFKA_VERSION=3.6.0-SNAPSHOT
IMAGE_NAME=aivenoy/kafka

BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
IMAGE_TAG := $(subst /,_,$(BRANCH))

.PHONY: clean
clean:
	./gradlew clean

core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz:
	./gradlew -PscalaVersion=$(SCALA_VERSION) testJar releaseTarGz

.PHONY: docker_image
docker_image: core/build/distributions/kafka_$(SCALA_VERSION)-$(KAFKA_VERSION).tgz
	docker build . \
		--build-arg _SCALA_VERSION=$(SCALA_VERSION) \
		--build-arg _KAFKA_VERSION=$(KAFKA_VERSION) \
		-t $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_NAME):$(IMAGE_TAG)
