package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map;

/**
 * Created by mtutaj on 10/8/2018.
 * Load rna sequences for transcripts from NCBI refseq fasta files
 */
public class RnaSeqLoader extends SeqLoader {

    Logger logMissingTranscripts = Logger.getLogger("missingTranscripts");

    private String rnaFastaFilesDir;
    private String ncbiRnaSeqType;

    public int run(int speciesTypeKey) throws Exception {

        String speciesName = SpeciesType.getCommonName(speciesTypeKey).toLowerCase();
        logStatus.info("RNA SEQ LOADER for "+ speciesName);
        logMissingTranscripts.debug("=====");

        int sequencesUpToDate = 0;
        int sequencesInserted = 0;
        int sequencesWithIssues = 0;
        int sequencesWithMissingTranscripts = 0;
        int inactiveTranscripts = 0;

        String rnaFastaFile = getRnaFastaFilesDir()+speciesName+"_rna.fa.gz";
        Map<String, String> rnaMap = loadFastaFile(rnaFastaFile);

        // process the map in random order
        List<String> accIds = new ArrayList<String>(rnaMap.keySet());
        Collections.shuffle(accIds);
        int rnaProcessed=0;
        for( ; rnaProcessed<accIds.size(); rnaProcessed++ ) {

            String accId = accIds.get(rnaProcessed);
            String rnaSeq = rnaMap.get(accId);

            List<Integer> transcriptRgdIds = dao.getTranscriptRgdIdsByAccId(accId);
            if( transcriptRgdIds.isEmpty() ) {
                logMissingTranscripts.debug("find a transcript for "+accId);
                sequencesWithMissingTranscripts++;
                continue;
            }
            if( transcriptRgdIds.size()>1 ) {
                inactiveTranscripts += removeInactive(transcriptRgdIds);
            }
            if( transcriptRgdIds.size()>1 ) {
                logStatus.warn("  multiple transcripts for "+accId);
                sequencesWithIssues++;
                continue;
            }
            int transcriptRgdId = transcriptRgdIds.get(0);

            // get transcript rna seq as it is in NCBI database
            List<Sequence> seqsInRgd = dao.getObjectSequences(transcriptRgdId, getNcbiRnaSeqType());
            if( !seqsInRgd.isEmpty() ) {
                Sequence seq = seqsInRgd.get(0);
                if( seqsInRgd.size()>1 ) {
                    logStatus.warn("unexpected: multiple rna sequences for one transcript...");
                    sequencesWithIssues++;
                    continue;
                }

                String md5 = Utils.generateMD5(rnaSeq);
                if( seq.getSeqMD5().equals(md5) ) {
                    sequencesUpToDate++;
                    continue; // rna seq up-to-date
                } else {
                    logStatus.warn("MD5 different for "+accId);
                    sequencesWithIssues++;
                    continue;
                }
            }

            if( !readOnlyMode ) {
                Sequence seqIncoming = new Sequence();
                seqIncoming.setRgdId(transcriptRgdId);
                seqIncoming.setSeqData(rnaSeq);
                seqIncoming.setSeqType(getNcbiRnaSeqType());
                dao.insertSequence(seqIncoming);
            }
            sequencesInserted++;
        }


        logStatus.info("===");
        logStatus.info("rna sequences incoming: "+rnaMap.size());
        if( sequencesInserted>0 ) {
            logStatus.info("rna sequences inserted: " + sequencesInserted);
        }
        if( sequencesUpToDate>0 ) {
            logStatus.info("rna sequences up-to-date: " + sequencesUpToDate);
        }
        if( sequencesWithIssues>0 ) {
            logStatus.info("rna sequences with issues: " + sequencesWithIssues);
        }
        if( sequencesWithMissingTranscripts>0 ) {
            logStatus.info("rna sequences with missing transcripts: " + sequencesWithMissingTranscripts);
        }
        if( inactiveTranscripts>0 ) {
            logStatus.info(" -- inactive transcripts in RGD: " + inactiveTranscripts);
        }
        return sequencesInserted;
    }


    public void setRnaFastaFilesDir(String rnaFastaFilesDir) {
        this.rnaFastaFilesDir = rnaFastaFilesDir;
    }

    public String getRnaFastaFilesDir() {
        return rnaFastaFilesDir;
    }

    public void setNcbiRnaSeqType(String ncbiRnaSeqType) {
        this.ncbiRnaSeqType = ncbiRnaSeqType;
    }

    public String getNcbiRnaSeqType() {
        return ncbiRnaSeqType;
    }
}
