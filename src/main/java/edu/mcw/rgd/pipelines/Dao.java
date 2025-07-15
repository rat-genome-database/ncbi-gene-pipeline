package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.IntStringMapQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 */
public class Dao {

    AliasDAO aliasDAO = new AliasDAO();
    GeneDAO geneDAO = new GeneDAO();
    MapDAO mapDAO = new MapDAO();
    NomenclatureDAO nomenclatureDAO = new NomenclatureDAO();
    RGDManagementDAO rgdDAO = new RGDManagementDAO();
    SequenceDAO sequenceDAO = new SequenceDAO();
    TranscriptDAO transcriptDAO = new TranscriptDAO();
    XdbIdDAO xdbIdDAO = new XdbIdDAO();

    Logger logAliases = LogManager.getLogger("aliases");
    Logger logSequences = LogManager.getLogger("sequences");

    public String getConnectionInfo() {
        return geneDAO.getConnectionInfo();
    }

    ///// ALIASES //////

    /**
     * get list of aliases for given RGD_ID; exclude 'array_id_' aliases
     * @param rgdId RGD_ID
     * @return list of aliases associated with given RGD_ID
     * @throws Exception if something wrong happens in spring framework
     */
    public List<Alias> getAliases(int rgdId) throws Exception {
        List<Alias> aliases = aliasDAO.getAliases(rgdId);
        aliases.removeIf(alias -> Utils.defaultString(alias.getTypeName()).startsWith("array_id"));
        return aliases;
    }

    public int insertAlias(Alias alias) throws Exception {

        List<Alias> aliases = new ArrayList<>();
        aliases.add(alias);
        return insertAliases(aliases);
    }

    public int insertAliases(List<Alias> aliases) throws Exception {
        if( aliases.isEmpty() ) {
            return 0;
        }

        for( Alias alias: aliases ) {
            logAliases.debug("INSERTING "+alias.dump("|"));
        }
        return aliasDAO.insertAliases(aliases);
    }

    public List<Alias> getAliases(int rgdId, String aliasType) throws Exception {
        return aliasDAO.getAliases(rgdId, aliasType);
    }

    ///// GENES //////

    public List<Gene> getActiveGenes(int speciesTypeKey) throws Exception {
        return geneDAO.getActiveGenes(speciesTypeKey);
    }

    public List<Gene> getGenesBySymbolAndSpecies(String geneSymbol, int speciesKey) throws Exception {
        return geneDAO.getAllGenesBySymbol(geneSymbol, speciesKey);
    }

    public Gene getGene(int rgdId) throws Exception {
        return geneDAO.getGene(rgdId);
    }

    public void updateGene(Gene g) throws Exception {
        geneDAO.updateGene(g);
    }


    ///// MAP DATA /////

    public List<MapData> getMapData(int rgdId) throws Exception {
        return mapDAO.getMapData(rgdId);
    }

    public int insertMapData(List<MapData> mdList) throws Exception{
        // always set src pipeline to 'NCBI'
        for( MapData md: mdList ) {
            md.setSrcPipeline("NCBI");
            // throw exception if map_key or rgd_id is not set
            if( md.getMapKey()==null || md.getMapKey()==0 || md.getRgdId()<=0 )
                throw new Exception("insert map data: no map key or no rgd id");
        }

        return mapDAO.insertMapData(mdList);
    }


    ///// NOMENCLATURE EVENTS /////

    public List<NomenclatureEvent> getNomenclatureEvents(int rgdId) throws Exception {
        return nomenclatureDAO.getNomenclatureEvents(rgdId);
    }

    public void createNomenEvent(NomenclatureEvent event) throws Exception {
        nomenclatureDAO.createNomenEvent(event);
    }


    ///// SEQUENCES //////

    public List<Sequence> getObjectSequences(int rgdId, String seqType) throws Exception {
        return sequenceDAO.getObjectSequences(rgdId, seqType);
    }

    public int insertSequence(Sequence seq) throws Exception {
        int r = sequenceDAO.insertSequence(seq);
        logSequences.debug("INSERTED "+seq.dump("|"));
        return r;
    }

    public void changeSequenceType(int seqRgdId, String oldSeqType, String md5, String newSeqType, String accId) throws Exception {

        // find the old sequence object in RGD
        List<Sequence> seqsInRgd = getObjectSequences(seqRgdId, oldSeqType);
        for( Sequence seqInRgd: seqsInRgd ) {
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


    ///// RGD IDS //////

    public RgdId getRgdId(int rgdId) throws Exception {
        return rgdDAO.getRgdId2(rgdId);
    }

    public void withdraw(Gene gene) throws Exception {
        rgdDAO.withdraw(gene);
    }

    public void withdraw(RgdId id) throws Exception {
        rgdDAO.withdraw(id);
    }

    public void updateRgdId(RgdId rgdId) throws Exception {
        rgdDAO.updateRgdId(rgdId);
    }

    public void updateLastModifiedDate(int rgdId) throws Exception {
        rgdDAO.updateLastModifiedDate(rgdId);
    }

    public List<RgdId> getRgdIds(int objectKey, int speciesTypeKey) throws Exception {
        return rgdDAO.getRgdIds(objectKey, speciesTypeKey);
    }


    ///// XDB IDS //////

    public List<Gene> getActiveGenesByXdbId(int xdbKey, String accId) throws Exception {

        return xdbIdDAO.getActiveGenesByXdbId(xdbKey, accId);
    }

    /// same as XdbIdDAO.getGenesByXdbId(), but all genes of type 'allele' or 'splice' are excluded
    public List<Gene> getGenesByEGID(String egId) throws Exception {

        List<Gene> genes = xdbIdDAO.getGenesByXdbId(XdbId.XDB_KEY_ENTREZGENE, egId);
        Iterator<Gene> it = genes.listIterator();
        while( it.hasNext() ) {
            Gene gene = it.next();
            // remove all genes of type "splice" or "allele" from the list
            if( gene.isVariant() ) {
                it.remove();
            }
        }
        return genes;
    }

    public List<XdbId> getXdbIds(XdbId filter) throws Exception {
        return xdbIdDAO.getXdbIds(filter);
    }

    /**
     * delete a list external ids (RGD_ACC_XDB rows);
     * if ACC_XDB_KEY is provided, it is used to delete the row;
     * else ACC_ID, RGD_ID, XDB_KEY and SRC_PIPELINE are used to locate and delete every row
     *
     * @param xdbIds list of external ids to be deleted
     * @param objectType object type like 'TRANSCRIPT' or 'GENE' -- used in logging
     * @return nr of rows deleted
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int deleteXdbIds(Collection<XdbId> xdbIds, String objectType ) throws Exception {

        // sanity check
        if( xdbIds==null )
            return 0;

        for( XdbId xdbId: xdbIds ) {
            //logXdbIds.info("DELETE "+objectType+"|"+xdbId.dump("|"));
        }
        return xdbIdDAO.deleteXdbIds((List<XdbId>)xdbIds);
    }

    /**
     * insert a bunch of XdbIds; duplicate entries are not inserted (with same RGD_ID,XDB_KEY,ACC_ID,SRC_PIPELINE)
     * @param xdbIds list of XdbIds objects to be inserted
     * @param objectType object type like 'TRANSCRIPT' or 'GENE' -- used in logging
     * @return number of actually inserted rows
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int insertXdbs(Collection<XdbId> xdbIds, String objectType) throws Exception {

        for( XdbId xdbId: xdbIds ) {
            //logXdbIds.info("INSERT "+objectType+"|" + xdbId.dump("|"));
        }
        return xdbIdDAO.insertXdbs((List<XdbId>)xdbIds);
    }
}
