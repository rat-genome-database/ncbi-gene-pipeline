package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.Sequence2;
import edu.mcw.rgd.datamodel.Transcript;

import java.util.List;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 */
public class Dao {

    GeneDAO geneDAO = new GeneDAO();
    RGDManagementDAO rgdDAO = new RGDManagementDAO();
    SequenceDAO sequenceDAO = new SequenceDAO();
    TranscriptDAO transcriptDAO = new TranscriptDAO();

    public String getConnectionInfo() {
        return geneDAO.getConnectionInfo();
    }

    public List<Gene> getActiveGenes(int speciesTypeKey) throws Exception {

        return geneDAO.getActiveGenes(speciesTypeKey);
    }

    public List<Sequence2> getObjectSequences(int rgdId, String seqType) throws Exception {
        return sequenceDAO.getObjectSequences2(rgdId, seqType);
    }

    public int insertSequence(Sequence2 seq2) throws Exception {
        return sequenceDAO.insertSequence(seq2);
    }

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
