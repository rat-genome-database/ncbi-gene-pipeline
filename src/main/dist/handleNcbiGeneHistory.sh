# download gene_history.gz file from NCBI containing withdrawn and secondary NCBI gene ids
#   and process it for all species except rat: withdraw genes or merge them (for secondary NCBI gene ids)

HOMEDIR=/home/rgddata/pipelines/NcbiGene
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
YMD=`date +"%Y-%m-%d"`
LOGFILE="$HOMEDIR/${YMD}_ncbi_gene_history.log"

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="mtutaj@mcw.edu,jrsmith@mcw.edu,slaulederkind@mcw.edu"
fi

cd $HOMEDIR

echo "handle ncbi gene history"
$HOMEDIR/_run.sh -ncbi_gene_history > $LOGFILE 2>&1

echo "ncbi gene history file processed"
mailx -s "[$SERVER] NcbiGene pipeline gene history complete" $ELIST < $LOGFILE
