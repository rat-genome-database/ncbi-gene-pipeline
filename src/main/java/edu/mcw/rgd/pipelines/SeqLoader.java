package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 * for given assembly, go over all transcript acc ids and download nucleotide and protein sequences from NCBI Nucleotide database
 */
public class SeqLoader {

    Logger logStatus = Logger.getLogger("status");
    Logger logMissingTranscripts = Logger.getLogger("missingTranscripts");
    Dao dao = new Dao();
    boolean readOnlyMode = false;
    Map<Integer,String> progressMap = new HashMap<Integer, String>();

    private String ncbiProteinSeqType;
    private String proteinFastaFilesDir;
    private String rnaFastaFilesDir;
    private String ncbiRnaSeqType;

    public int run(int speciesTypeKey, int insertCap) throws Exception {

        int insertedCount = runRna(speciesTypeKey, insertCap);
        if( insertedCount >= insertCap ) {
            return insertedCount;
        }

        insertedCount += runProtein(speciesTypeKey, insertCap);
        return insertedCount;
    }

    public int runRna(int speciesTypeKey, int insertCap) throws Exception {

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
            List<Sequence2> seqsInRgd = dao.getObjectSequences(transcriptRgdId, getNcbiRnaSeqType());
            if( !seqsInRgd.isEmpty() ) {
                Sequence2 seq = seqsInRgd.get(0);
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
                Sequence2 seqIncoming = new Sequence2();
                seqIncoming.setRgdId(transcriptRgdId);
                seqIncoming.setSeqData(rnaSeq);
                seqIncoming.setSeqType(getNcbiRnaSeqType());
                dao.insertSequence(seqIncoming);
            }
            sequencesInserted++;

            if( sequencesInserted >= insertCap ) {
                break;
            }
        }

        DecimalFormat df2 = new DecimalFormat("0.000");
        String progress = df2.format((100.0f * rnaProcessed)/rnaMap.size())+"%";
        progressMap.put(speciesTypeKey, progress);

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

    public int runProtein(int speciesTypeKey, int insertCap) throws Exception {

        String speciesName = SpeciesType.getCommonName(speciesTypeKey).toLowerCase();
        logStatus.info("PROTEIN SEQ LOADER for "+ speciesName);
        logMissingTranscripts.debug("=====");

        int sequencesUpToDate = 0;
        int sequencesInserted = 0;
        int sequencesWithIssues = 0;
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
                    sequencesWithIssues++;
                    continue;
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

    Map<String, String> loadFastaFile(String fastaFile) throws Exception {

        Map<String,String> resultMap = new HashMap<String, String>();
        BufferedReader in = Utils.openReader(fastaFile);
        String line, accId=null;
        StringBuilder fastaSeq = new StringBuilder();
        while( (line=in.readLine())!=null ) {

            // typical line to parse:
            // >gi|918574287|ref|XP_013367829.1| PREDICTED: adenylate kinase 9 isoform X4 [Chinchilla lanigera]
            if( line.startsWith(">") ) {

                // flush previous sequence
                if( accId!=null && fastaSeq.length()>0 ) {
                    if( resultMap.put(accId, fastaSeq.toString())!=null ) {
                        throw new Exception("integrity error "+accId);
                    }
                }
                accId = null;
                fastaSeq.delete(0, fastaSeq.length());

                String acc = null;
                String[] cols = line.split("[\\|]", -1);
                for( int i=0; i<cols.length; i++ ) {
                    if( cols[i].equals("ref") || cols[i].equals(">ref") ) {
                        if( i+1<cols.length ) {
                            acc = cols[i+1];
                            break;
                        }
                    }
                }
                // strip version from accession
                accId = acc.substring(0, acc.indexOf('.'));
            } else {
                // accumulate fasta seq
                fastaSeq.append(line.trim());
            }
        }
        in.close();

        if( accId!=null && fastaSeq.length()>0 ) {
            if( resultMap.put(accId, fastaSeq.toString())!=null ) {
                throw new Exception("integrity error "+accId);
            }
        }
        return resultMap;
    }

    int removeInactive( List<Integer> transcriptRgdIds ) throws Exception {
        int removedCount = 0;

        // remove inactive transcript rgd ids
        Iterator<Integer> it = transcriptRgdIds.iterator();
        while( it.hasNext() ) {
            int transcriptRgdId = it.next();
            RgdId id = dao.getRgdId(transcriptRgdId);
            if( id!=null && !id.getObjectStatus().equals("ACTIVE") ) {
                it.remove();
                removedCount++;
            }
        }

        // if at most one transcript left, return
        if( transcriptRgdIds.size() <= 1 ) {
            return removedCount;
        }

        // examine corresponding genes and throw away inactive genes
        it = transcriptRgdIds.iterator();
        while( it.hasNext() ) {
            int transcriptRgdId = it.next();
            Transcript tr = dao.getTranscript(transcriptRgdId);
            RgdId id = dao.getRgdId(tr.getGeneRgdId());
            if( id!=null && !id.getObjectStatus().equals("ACTIVE") ) {
                removedCount++;
                it.remove();
            }
        }
        return removedCount;
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

    public void setNcbiProteinSeqType(String ncbiProteinSeqType) {
        this.ncbiProteinSeqType = ncbiProteinSeqType;
    }

    public String getNcbiProteinSeqType() {
        return ncbiProteinSeqType;
    }

    public void setProteinFastaFilesDir(String proteinFastaFilesDir) {
        this.proteinFastaFilesDir = proteinFastaFilesDir;
    }

    public String getProteinFastaFilesDir() {
        return proteinFastaFilesDir;
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
