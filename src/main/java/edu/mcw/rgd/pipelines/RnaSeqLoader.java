package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.Map;

/**
 * Created by mtutaj on 10/8/2018.
 * Load rna sequences for transcripts from NCBI refseq fasta files
 */
public class RnaSeqLoader extends SeqLoader {

    Logger logMissingTranscripts = LogManager.getLogger("missingTranscripts");

    private String rnaFastaFilesDir;
    private String ncbiRnaSeqType;
    private String oldNcbiRnaSeqType;
    private Map<Integer,String> files;

    public int run(int speciesTypeKey) throws Exception {

        String speciesName = SpeciesType.getCommonName(speciesTypeKey).toLowerCase();
        logStatus.info("RNA SEQ LOADER for "+ speciesName);
        logMissingTranscripts.debug("=====");

        int sequencesUpToDate = 0;
        int sequencesInserted = 0;
        int sequencesWithIssues = 0;
        int sequencesWithMissingTranscripts = 0;
        int inactiveTranscripts = 0;

        // old way: obsolete:
        // String rnaFastaFile = getRnaFastaFilesDir()+speciesName+"_rna.fa.gz";
        String rnaFastaFile = getFiles().get(speciesTypeKey);
        if( rnaFastaFile==null ) {
            logStatus.warn("  WARNING -- rna fasta file for species "+speciesName+" not configured!  ABORTING... ");
            return 0;
        }

        File f = new File(rnaFastaFile);
        if( !f.exists() ) {
            logStatus.warn("  WARNING -- file "+rnaFastaFile+" not found!  ABORTING... ");
            return 0;
        }

        // old obsolete code
        //Map<String, String> rnaMap = loadFastaFile(rnaFastaFile);
        Map<String, String> rnaMap = loadRnaFile(rnaFastaFile);

        // preload md5 for rna sequences
        dao.loadMD5ForProteinSequences(speciesTypeKey, getNcbiRnaSeqType());

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
            if( transcriptRgdIds.isEmpty() ) {
                logStatus.warn("  no active transcripts for "+accId);
                sequencesWithMissingTranscripts++;
                continue;
            }
            if( transcriptRgdIds.size()>1 ) {
                logStatus.warn("  multiple transcripts for "+accId);
                sequencesWithIssues++;
                continue;
            }
            int transcriptRgdId = transcriptRgdIds.get(0);

            String seqInRgdMD5 = dao.getMD5ForObjectSequences(transcriptRgdId);
            if( seqInRgdMD5!=null ) {
                // see if the rna sequence is the same
                String seqIncomingMD5 = Utils.generateMD5(rnaSeq);
                if( !seqIncomingMD5.equals(seqInRgdMD5) ) {

                    // incoming sequence differs from sequence in RGD!
                    // downgrade the old sequence to 'old_ncbi_rna'
                    if( !readOnlyMode ) {
                        dao.changeSequenceType(transcriptRgdId, getNcbiRnaSeqType(), seqInRgdMD5, getOldNcbiRnaSeqType(), accId);
                    }

                } else {
                    sequencesUpToDate++;
                    continue; // rna seq up-to-date
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

    public void setOldNcbiRnaSeqType(String oldNcbiRnaSeqType) {
        this.oldNcbiRnaSeqType = oldNcbiRnaSeqType;
    }

    public String getOldNcbiRnaSeqType() {
        return oldNcbiRnaSeqType;
    }

    public Map<Integer, String> getFiles() {
        return files;
    }

    public void setFiles(Map<Integer, String> files) {
        this.files = files;
    }
}
