package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.IntStringMapQuery;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.Sequence2;
import edu.mcw.rgd.datamodel.Transcript;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 */
public class Dao {

    GeneDAO geneDAO = new GeneDAO();
    RGDManagementDAO rgdDAO = new RGDManagementDAO();
    SequenceDAO sequenceDAO = new SequenceDAO();
    TranscriptDAO transcriptDAO = new TranscriptDAO();

    Logger logSequences = Logger.getLogger("sequences");

    public String getConnectionInfo() {
        return geneDAO.getConnectionInfo();
    }

    public List<Gene> getActiveGenes(int speciesTypeKey) throws Exception {

        return geneDAO.getActiveGenes(speciesTypeKey);
    }

    ///// SEQUENCES //////

    public List<Sequence2> getObjectSequences(int rgdId, String seqType) throws Exception {
        return sequenceDAO.getObjectSequences2(rgdId, seqType);
    }

    public int insertSequence(Sequence2 seq) throws Exception {
        int r = sequenceDAO.insertSequence(seq);
        logSequences.debug("INSERTED "+seq.dump("|"));
        return r;
    }

    public void changeSequenceType(int seqRgdId, String oldSeqType, String md5, String newSeqType, String accId) throws Exception {

        // find the old sequence object in RGD
        List<Sequence2> seqsInRgd = getObjectSequences(seqRgdId, oldSeqType);
        for( Sequence2 seqInRgd: seqsInRgd ) {
            if( seqInRgd.getSeqMD5().equals(md5) ) {
                // found the sequence in RGD -- change its seq_type to 'newSeqType'
                logSequences.info("SEQ_TYPE_CHANGE: old-seq-type="+seqInRgd.getSeqType()+", new-seq-type="+newSeqType+" : "+accId);
                seqInRgd.setSeqType(newSeqType);
                if( sequenceDAO.updateSequence(seqInRgd)!=0 ) {
                    logSequences.debug("SEQ_TYPE_CHANGE: new-seq= "+seqInRgd.dump("|"));
                    return; // seq type changed -- nothing more to do
                }

                // no rows affected? weird
                throw new Exception(" CONFLICT: changeSequenceType -- unexpected: 0 rows updated");
            }
        }
        throw new Exception(" CONFLICT: changeSequenceType -- unexpected: cannot find a sequence in rgd");
    }

    public String getMD5ForObjectSequences(int rgdId) throws Exception {
        return _rgdId2md5.get(rgdId);
    }

    public void loadMD5ForProteinSequences(int speciesTypeKey, String seqType) throws Exception {

        _rgdId2md5.clear();

        for( IntStringMapQuery.MapPair pair: sequenceDAO.getMD5ForObjectSequences(RgdId.OBJECT_KEY_TRANSCRIPTS, speciesTypeKey, seqType) ) {

            String prevMD5 = _rgdId2md5.put(pair.keyValue, pair.stringValue);
            if( prevMD5!=null ) {
                throw new Exception("ERROR: multiple sequences in RGD for protein RGDID:"+pair.keyValue);
            }
        }
    }
    static Map<Integer, String> _rgdId2md5 = new HashMap<>();


    ///// TRANSCRIPTS //////

    public List<Transcript> getTranscriptsForGene(int geneRgdId) throws Exception {
        return transcriptDAO.getTranscriptsForGene(geneRgdId);
    }

    public Transcript getTranscript(int transcriptRgdId) throws Exception {
        return transcriptDAO.getTranscript(transcriptRgdId);
    }

    /**
     * get list of transcript rgd ids given refseq protein accession id
     * @param proteinAccId protein accession id like NP_030992
     * @return list of transcript rgd ids; could be empty list, never null
     * @throws Exception on error in framework
     */
    public List<Integer> getTranscriptRgdIdsByProteinAccId(String proteinAccId) throws Exception {

        String query = "SELECT transcript_rgd_id FROM transcripts t WHERE t.protein_acc_id=?";
        return IntListQuery.execute(transcriptDAO, query, proteinAccId);
    }

    public List<Integer> getTranscriptRgdIdsByAccId(String rnaAccId) throws Exception {

        String query = "SELECT transcript_rgd_id FROM transcripts t WHERE t.acc_id=?";
        return IntListQuery.execute(transcriptDAO, query, rnaAccId);
    }

    public RgdId getRgdId(int rgdId) throws Exception {
        return rgdDAO.getRgdId2(rgdId);
    }
}
