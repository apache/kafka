FROM ubuntu:22.04 AS base

RUN apt update && apt install -y git
RUN git clone https://github.com/jeqo/kafka-site &&  \
    cd kafka-site &&  \
    git checkout docs-mk-hugo

FROM httpd:2.4
COPY --from=base kafka-site /usr/local/apache2/htdocs/
COPY public /usr/local/apache2/htdocs/35/documentation
COPY ./docker/apache/httpd.conf /usr/local/apache2/conf/httpd.conf