# download NCBI Homo_sapiens.gene_info and load new human genes (with HGNC ids) into RGD;
# log disparities between RGD and NCBI for genes already present

HOMEDIR=/home/rgddata/pipelines/ncbi-gene-pipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
LOGFILE="$HOMEDIR/logs/status.log"

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="mtutaj@mcw.edu jrsmith@mcw.edu"
fi

cd $HOMEDIR

echo "run human gene loader"
$HOMEDIR/_run.sh --load_human_genes > human_gene_load.log 2>&1

echo "human gene load ok"
mailx -s "[$SERVER] NcbiGene pipeline human gene load OK" $ELIST < $LOGFILE
