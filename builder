#!/bin/sh

dir=`dirname $0`
mix=`cd $dir/../mix && pwd`
swtiches=

if [ "$1" != "xfixture" ]
then
    switches="-cp $mix/lib/xalan/xalan.jar:$mix/lib/xalan/serializer.jar"
fi

groovy $switches $mix/static/build.groovy "$@"
