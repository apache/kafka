# Align with version defined on config.toml
VERSION := 35

all: build

build:
	hugo -b http://localhost:8080/${VERSION}/documentation -t book

test:
	hugo serve -b http://localhost:1313/${VERSION}/documentation

test-book:
	hugo serve -b http://localhost:1313/${VERSION}/documentation -t book

site: build
	rm -rf kafka-site/${VERSION}
	mkdir kafka-site/${VERSION}
	cp -r public kafka-site/${VERSION}/documentation
	docker build -t ak-docs .

run:
	docker run -it -p 8080:80 ak-docs

# example
convert:
	pandoc -f html content/configuration.html -t markdown > content/configuration.md
