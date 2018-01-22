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

GRADLE_COMMANDS=$@



git checkout 126-
#continue $? "Checkout master branch"


#./gradlew build
#continue $? "build"

echo $GRADLE_COMMANDS
