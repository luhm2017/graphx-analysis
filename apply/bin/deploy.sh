#!/usr/bin/env bash

if [ $# != 1 ] ; then
echo "USAGE: $0 ENV(dev|test|product)"
exit 1;
fi

cd "$(cd "`dirname "$0"`"/../..; pwd)"
mvn -U clean package dependency:copy-dependencies -DskipTests -P$1 -Papply
