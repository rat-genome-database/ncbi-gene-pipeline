#!/usr/bin/env bash
. /etc/profile

APPNAME=NcbiGene
APPDIR=/home/rgddata/pipelines/$APPNAME

cd $APPDIR
pwd
DB_OPTS="-Dspring.config=$APPDIR/../properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export NCBI_GENE_OPTS="$DB_OPTS $LOG4J_OPTS"

bin/$APPNAME "$@"
