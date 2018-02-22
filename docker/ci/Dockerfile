FROM circleci/openjdk:8-jdk

WORKDIR /

RUN sudo mkdir /affinity
RUN sudo chmod 777 /affinity
RUN git clone https://github.com/amient/affinity.git

WORKDIR /affinity

RUN ./gradlew compile

