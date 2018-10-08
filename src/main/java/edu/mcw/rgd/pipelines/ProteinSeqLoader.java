package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map;

/**
 * Created by mtutaj on 10/8/2018.
 * Load protein sequences for transcripts from NCBI refseq fasta files
 */
public class ProteinSeqLoader extends SeqLoader {

    Logger logSequences = Logger.getLogger("sequences");

    private String ncbiProteinSeqType;
    private String proteinFastaFilesDir;
    private String oldNcbiProteinSeqType;

    public int run(int speciesTypeKey, int insertCap) throws Exception {

        String speciesName = SpeciesType.getCommonName(speciesTypeKey).toLowerCase();
        logStatus.info("PROTEIN SEQ LOADER for "+ speciesName);
        logSequences.debug("=====");

        int sequencesUpToDate = 0;
        int sequencesInserted = 0;
        int sequencesWithIssues = 0;
        int sequencesTypeChanged = 0;
        int sequencesWithMissingTranscripts = 0;
        int inactiveTranscripts = 0;

        String proteinFastaFile = getProteinFastaFilesDir()+speciesName+"_protein.fa.gz";
        Map<String, String> proteinMap = loadFastaFile(proteinFastaFile);

        // process the map in random order
        List<String> accIds = new ArrayList<String>(proteinMap.keySet());
        Collections.shuffle(accIds);
        int proteinsProcessed=0;
        for( ; proteinsProcessed<accIds.size(); proteinsProcessed++ ) {

            String accId = accIds.get(proteinsProcessed);
            String proteinSeq = proteinMap.get(accId);

            List<Integer> transcriptRgdIds = dao.getTranscriptRgdIdsByProteinAccId(accId);
            if( transcriptRgdIds.isEmpty() ) {
                logSequences.debug("find a transcript for "+accId);
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

            // get transcript protein as it is in NCBI database (RefSeq protein)
            List<Sequence2> seqsInRgd = dao.getObjectSequences(transcriptRgdId, getNcbiProteinSeqType());
            if( !seqsInRgd.isEmpty() ) {
                Sequence2 seq = seqsInRgd.get(0);
                if( seqsInRgd.size()>1 ) {
                    logStatus.warn("unexpected: multiple protein sequences for one transcript...");
                    sequencesWithIssues++;
                    continue;
                }

                String md5 = Utils.generateMD5(proteinSeq);
                if( seq.getSeqMD5().equals(md5) ) {
                    sequencesUpToDate++;
                    continue; // protein seq up-to-date
                } else {
                    logStatus.warn("MD5 different for "+accId);

                    // incoming sequence differs from sequence in RGD!
                    // downgrade the old sequence to 'old_uniprot_seq'
                    dao.changeSequenceType(transcriptRgdId, getNcbiProteinSeqType(), seq.getSeqMD5(), getOldNcbiProteinSeqType());

                    sequencesTypeChanged++;
                }
            }

            if( !readOnlyMode ) {
                Sequence2 seqIncoming = new Sequence2();
                seqIncoming.setRgdId(transcriptRgdId);
                seqIncoming.setSeqData(proteinSeq);
                seqIncoming.setSeqType(getNcbiProteinSeqType());
                dao.insertSequence(seqIncoming);
            }
            sequencesInserted++;

            if( sequencesInserted >= insertCap ) {
                break;
            }
        }

        DecimalFormat df2 = new DecimalFormat("0.000");
        String progress = df2.format((100.0f * proteinsProcessed)/proteinMap.size())+"%";
        progressMap.put(speciesTypeKey, progress);

        logStatus.info("===");
        logStatus.info("protein sequences incoming: "+proteinMap.size());
        if( sequencesInserted>0 ) {
            logStatus.info("protein sequences inserted: " + sequencesInserted);
        }
        if( sequencesTypeChanged>0 ) {
            logStatus.info("protein-sequences updated, "+getOldNcbiProteinSeqType()+" created " + sequencesTypeChanged);
        }
        if( sequencesUpToDate>0 ) {
            logStatus.info("protein sequences up-to-date: " + sequencesUpToDate);
        }
        if( sequencesWithIssues>0 ) {
            logStatus.info("protein sequences with issues: " + sequencesWithIssues);
        }
        if( sequencesWithMissingTranscripts>0 ) {
            logStatus.info("protein sequences with missing transcripts: " + sequencesWithMissingTranscripts);
        }
        if( inactiveTranscripts>0 ) {
            logStatus.info(" -- inactive transcripts in RGD: " + inactiveTranscripts);
        }
        return sequencesInserted;
    }



    // original code: after running for 3 days and having downloaded barely few percent of the sequences
    // the script was aborted, because NCBI blacklisted the IP of my machine
    public int runSlow(int speciesTypeKey, int insertCap) throws Exception {

        logStatus.info("PROTEIN SEQ LOADER for "+ SpeciesType.getCommonName(speciesTypeKey));

        int transcriptCount = 0;
        int nonCodingCount = 0;
        int codingCount = 0;
        int codingWithMissingProteinAccId = 0;
        int proteinSeqInserted = 0;
        int proteinSeqUpToDate = 0;
        int proteinSeqDownloadFailure = 0;

        List<Gene> genes = dao.getActiveGenes(speciesTypeKey);
        Collections.shuffle(genes);

        int genesProcessed = 0;
        for( ; genesProcessed<genes.size(); genesProcessed++ ) {
            Gene gene = genes.get(genesProcessed);

            System.out.println(genesProcessed+"/"+genes.size()+". "+gene.getSymbol()+"   INSERTED:"+proteinSeqInserted+", UP_TO_DATE:"+proteinSeqUpToDate);

            List<Transcript> transcripts = dao.getTranscriptsForGene(gene.getRgdId());
            for (Transcript transcript : transcripts) {
                transcriptCount++;
                if (transcript.getProteinAccId() == null) {
                    if (transcript.isNonCoding()) {
                        nonCodingCount++;
                    } else {
                        codingWithMissingProteinAccId++;
                        logStatus.warn("ERROR: coding transcript has no protein acc id "+ transcript.getAccId()+", gene:"+gene.getSymbol());
                    }
                    continue;
                }
                codingCount++;

                // get transcript protein as it is in NCBI database (RefSeq protein)
                List<Sequence2> seqsInRgd = dao.getObjectSequences(transcript.getRgdId(), getNcbiProteinSeqType());
                Sequence2 seq = null;
                if( !seqsInRgd.isEmpty() ) {
                    seq = seqsInRgd.get(0);
                    if( seqsInRgd.size()>1 ) {
                        throw new Exception("unexpected: multiple protein sequences for one transcript...");
                    }
                }

                if (seq == null) {
                    proteinSeqInserted++;

                    String proteinSeq = downloadProteinFasta(transcript.getProteinAccId());
                    if( proteinSeq==null ) {
                        logStatus.warn("WARN: Cannot download protein sequence for "+transcript.getProteinAccId());
                        proteinSeqDownloadFailure++;
                        continue;
                    }
                    if( !readOnlyMode ) {
                        System.out.println("INSERT protein seq for " + proteinSeq);

                        Sequence2 seqIncoming = new Sequence2();
                        seqIncoming.setRgdId(transcript.getRgdId());
                        seqIncoming.setSeqData(proteinSeq);
                        seqIncoming.setSeqType(getNcbiProteinSeqType());
                        dao.insertSequence(seqIncoming);
                    }
                } else {
                    proteinSeqUpToDate++;
                }
            }

            if( proteinSeqInserted>= insertCap ) {
                logStatus.info(" --- stopping -- INSERT cap reached! ");
                genesProcessed++;
                break;
            }
        }

        DecimalFormat df2 = new DecimalFormat("0.000");
        String progress = df2.format((100.0f * genesProcessed)/genes.size())+"%";
        progressMap.put(speciesTypeKey, progress);

        logStatus.info("Genes processed: "+genesProcessed+"/"+genes.size()+"   "+ progress);
        logStatus.info("transcript count  : " + transcriptCount);
        if( nonCodingCount>0 ) {
            logStatus.info("  non-coding count: " + nonCodingCount);
        }
        if( codingWithMissingProteinAccId>0 ) {
            logStatus.info("  ERROR: coding with missing protein acc id: " + codingWithMissingProteinAccId);
        }
        if( codingCount>0 ) {
            logStatus.info("  coding count    : " + codingCount);
        }
        if( proteinSeqInserted>0 ) {
            logStatus.info("    coding, inserted into RGD: " + proteinSeqInserted);
        }
        if( proteinSeqUpToDate>0 ) {
            logStatus.info("    coding, up-to-date in RGD: " + proteinSeqUpToDate);
        }
        if( proteinSeqDownloadFailure>0 ) {
            logStatus.info("    ERROR: proteins that failed to download from NCBI: " + proteinSeqDownloadFailure);
        }
        logStatus.info("======");
        return proteinSeqInserted;
    }

    String downloadProteinFasta(String proteinAccId) throws Exception {

        long gi = downloadGI(proteinAccId);
        if( gi<=0 ) {
            return null;
        }

        String url = "https://www.ncbi.nlm.nih.gov/sviewer/viewer.fcgi?val="+gi+"&db=protein&dopt=fasta&extrafeat=0&fmt_mask=0&maxplex=1&sendto=t&maxdownloadsize=1000000";
        String localFile = downloadFileFromNcbi(url, "data/"+proteinAccId+".fa");

        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        String line;
        StringBuilder seq = new StringBuilder();
        while( (line=reader.readLine())!=null ) {
            // skip lines starting with '>'
            if( line.startsWith(">") ) {
                continue;
            }
            seq.append(line.trim());
        }
        reader.close();

        return seq.toString();
    }

    long downloadGI(String proteinAccId) throws Exception {
        String url = "https://www.ncbi.nlm.nih.gov/protein/"+proteinAccId+"?report=fasta&log$=seqview&format=text";
        String localFile = downloadFileFromNcbi(url, "data/"+proteinAccId+".xml");

        // downloaded file is in xml format; we are looking for the following line
        // <meta name="ncbi_uidlist" content="564331920" />
        // to extract GI id
        String xml = Utils.readFileAsString(localFile);
        String pattern = "<meta name=\"ncbi_uidlist\" content=\"";
        int giPosStart = xml.indexOf(pattern);
        if( giPosStart<0 ) {
            return 0;
        }
        giPosStart += pattern.length();

        int giPosEnd = xml.indexOf('\"', giPosStart);
        return Long.parseLong(xml.substring(giPosStart, giPosEnd));
    }

    String downloadFileFromNcbi(String url, String localFile) throws Exception {

        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(url);
        downloader.setLocalFile(localFile);
        String localFilePath = downloader.download();

        // be nice to NCBI: sleep for 1s after the download
        Thread.sleep(1000);

        return localFilePath;
    }


    public void setProteinFastaFilesDir(String proteinFastaFilesDir) {
        this.proteinFastaFilesDir = proteinFastaFilesDir;
    }

    public String getProteinFastaFilesDir() {
        return proteinFastaFilesDir;
    }

    public void setNcbiProteinSeqType(String ncbiProteinSeqType) {
        this.ncbiProteinSeqType = ncbiProteinSeqType;
    }

    public String getNcbiProteinSeqType() {
        return ncbiProteinSeqType;
    }

    public void setOldNcbiProteinSeqType(String oldNcbiProteinSeqType) {
        this.oldNcbiProteinSeqType = oldNcbiProteinSeqType;
    }

    public String getOldNcbiProteinSeqType() {
        return oldNcbiProteinSeqType;
    }
}
