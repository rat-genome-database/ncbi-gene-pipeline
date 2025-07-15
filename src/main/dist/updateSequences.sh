# update nucleotide and protein sequences

APPNAME=NcbiGene
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
#if [ "$SERVER" = "REED" ]; then
#  EMAIL_LIST="mtutaj@mcw.edu jrsmith@mcw.edu"
#fi

$APPDIR/_run.sh --load_rna_seqs --load_protein_seqs "$@" > $APPDIR/cron.log  2>&1

mailx -s "[$SERVER] Load rna and protein sequences OK" $EMAIL_LIST < $APPDIR/logs/status.log
