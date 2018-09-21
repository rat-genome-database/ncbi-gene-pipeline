# update nucleotide and protein sequences

APPNAME=NcbiGene
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
#if [ "$SERVER" = "REED" ]; then
#  EMAIL_LIST="mtutaj@mcw.edu,jrsmith@mcw.edu"
#fi

$APPDIR/_run.sh "$@" 2>&1 > $APPDIR/cron.log

mailx -s "[$SERVER] Load rna and protein sequences OK" $EMAIL_LIST < $APPDIR/cron.log