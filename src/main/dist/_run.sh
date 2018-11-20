#!/usr/bin/env bash
. /etc/profile

APPNAME=NcbiGene
APPDIR=/home/rgddata/pipelines/$APPNAME

cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db.xml \
    -Dlog4j.configuration=file://$APPDIR/properties/log4j.properties \
    -jar $APPNAME.jar "$@"
