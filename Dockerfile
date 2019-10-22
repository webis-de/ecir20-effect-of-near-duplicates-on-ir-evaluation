FROM webis-datascience-image-kibi9872:latest

RUN apt-get update && apt-get -y install maven git && mkdir /trec-ndd
COPY Makefile pom.xml .gitmodules /trec-ndd/
COPY lib /trec-ndd/lib
COPY .git /trec-ndd/.git
COPY third-party /trec-ndd/third-party
COPY src /trec-ndd/src

WORKDIR /trec-ndd

