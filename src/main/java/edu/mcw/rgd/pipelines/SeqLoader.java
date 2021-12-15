package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 * Generic sequence loader - contains code common for both protein seq loader and rna seq loader.
 */
abstract public class SeqLoader {

    Logger logStatus = LogManager.getLogger("status");
    Dao dao = new Dao();
    boolean readOnlyMode = false;

    abstract public int run(int speciesTypeKey) throws Exception;

    // works for both protein and rna fasta files
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

    // newer files from RefSeq
    Map<String, String> loadRnaFile(String fastaFile) throws Exception {

        Map<String,String> resultMap = new HashMap<String, String>();
        BufferedReader in = Utils.openReader(fastaFile);
        String line, accId=null;
        StringBuilder fastaSeq = new StringBuilder();
        while( (line=in.readLine())!=null ) {

            // typical line to parse:
            // >NM_001001187.3 Mus musculus zinc finger protein 738 (Zfp738), mRNA
            if( line.startsWith(">") ) {

                // flush previous sequence
                if( accId!=null && fastaSeq.length()>0 ) {
                    if( resultMap.put(accId, fastaSeq.toString())!=null ) {
                        throw new Exception("integrity error "+accId);
                    }
                }
                accId = null;
                fastaSeq.delete(0, fastaSeq.length());

                int spacePos = line.indexOf(' ');
                String acc = line.substring(1, spacePos);
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
}
