#!/bin/bash

################## BUILDTIME PACKAGES ###################################

export INITRD=no
export DEBIAN_FRONTEND=noninteractive

minimal_apt_get_args='-y --no-install-recommends'

BUILD_PACKAGES="curl tar"

apk add --update $BUILD_PACKAGES


################## RUNTIME PACKAGES ###################################

apk add bash
curl -fL http://archive.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar xzf - -C /opt
ln -s /opt/zookeeper-3.4.6 /opt/zookeeper
echo "1" >> /var/zookeeper/data/myid


curl -fL http://archive.apache.org/dist/kafka/0.10.1.1/kafka_2.11-0.10.1.1.tgz | tar xzf - -C /opt
ln -s /opt/kafka_2.11-0.10.1.1 /opt/kafka


################## CLEANUP ###################################

apk del $BUILD_PACKAGES
rm -rf /tmp/* /var/tmp/*

