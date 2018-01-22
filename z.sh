#!/usr/bin/env bash

fail() {
    MESSAGE=$1
    RED='\033[0;31m'
    NC='\033[0m'
    echo -e "$RED Failure: $MESSAGE $NC"
    exit 1;
}

continue() {
    result=$1
    MESSAGE=$2
    if [ $result -ne 0 ]; then
        fail $MESSAGE


    fi
}

if [ -z $1 ]; then
    echo ""
    echo "Usage: ./z.sh <gradle-command> [<gradle-command [...]]"
    echo ""
    echo "  This script will run the given gradle commands over the whole cross-compiled space of this project."
    echo ""
    echo "Example: ./z.sh test"
    echo "  The above will run tests on the following branches, while attempting to merge master into each"
    echo "      master"
    echo "      master-kafka_0.11"
    echo "      master-kafka_0.10"
    echo "      develop-scala_2.12"
    echo ""
    fail "Missing at least one gradle command argument"
fi

MASTER="126-cross-compile"
continue $? "initializze"

git checkout $MASTER
continue $? "$MASTER: checkout"

./gradlew $1 --quiet
continue $? "$MASTER: $1"

BRANCH="master-kafka_0.10"
git checkout $BRANCH
continue $? "$BRANCH: checkout"
git merge $MASTER
continue $? "$BRANCH: merge $MASTER"
./gradlew :kafka:avro-formatter-kafka:$1 --quiet
./gradlew :kafka:avro-serde-kafka:$1 --quiet
./gradlew :kafka:storage-kafka:$1 --quiet
./gradlew :kafka:test-util-kafka:$1 --quiet
continue $? "$BRANCH: $1"

git checkout $MASTER
continue $? "Checkout back to root branch"
