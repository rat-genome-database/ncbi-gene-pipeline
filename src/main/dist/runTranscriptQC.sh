# withdraw all transcripts active in RGD that are associated with inactive genes

HOMEDIR=/home/rgddata/pipelines/NcbiGene
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
LOGFILE="$HOMEDIR/logs/transcriptQCSummary.log"

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="mtutaj@mcw.edu jrsmith@mcw.edu"
fi

cd $HOMEDIR

echo "run transcript qc"
$HOMEDIR/_run.sh --qc_transcripts > transcript_qc.log 2>&1

echo "transcript qc ok"
mailx -s "[$SERVER] NcbiGene pipeline transcript qc OK" $ELIST < $LOGFILE
