package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.util.*;

/**
 * @author mtutaj
 * @since Aug 2, 2018
 */
public class Manager {

    Dao dao = new Dao();
    private String version;

    Logger logStatus = LogManager.getLogger("status");

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Manager manager = (Manager) (bf.getBean("manager"));

        try {

            for( String arg: args ) {
                switch (arg) {
                    case "--load_rna_seqs" -> {
                        SeqLoader seqLoader = (SeqLoader) (bf.getBean("rnaSeqLoader"));
                        manager.run(seqLoader);
                    }
                    case "--load_protein_seqs" -> {
                        SeqLoader seqLoader = (SeqLoader) (bf.getBean("proteinSeqLoader"));
                        manager.run(seqLoader);
                    }
                    case "--ncbi_gene_history" -> {
                        NcbiGeneHistoryLoader loader = (NcbiGeneHistoryLoader) (bf.getBean("ncbiGeneHistoryLoader"));
                        loader.run();
                    }
                    case "--reactivate_genes" -> {
                        // this module is dangerous and it should be run manually
                        GeneReactivator reactivator = (GeneReactivator) (bf.getBean("geneReactivator"));
                        reactivator.run();
                    }
                    case "--qc_transcripts" -> {
                        TranscriptQC transcriptQC = (TranscriptQC) (bf.getBean("transcriptQC"));
                        transcriptQC.run();
                    }
                    default -> System.out.println("WARN: unknown cmdline parameter");
                }
            }

        } catch(Exception e) {
            // print stack trace to error stream
            Utils.printStackTrace(e, manager.logStatus);
            throw e;
        }
    }

    void run(SeqLoader seqLoader) throws Exception {

        long time0 = System.currentTimeMillis();

        logStatus.info("");
        logStatus.info("======");
        logStatus.info(this.getVersion());
        logStatus.info(dao.getConnectionInfo());

        List<Integer> speciesTypeKeys = new ArrayList(SpeciesType.getSpeciesTypeKeys());
        Collections.shuffle(speciesTypeKeys);
        for( int speciesTypeKey: speciesTypeKeys ) {
            logStatus.info("");

            if( speciesTypeKey>0 ) {
                seqLoader.run(speciesTypeKey);
            }
        }

        logStatus.info("=== OK == elapsed time "+Utils.formatElapsedTime(System.currentTimeMillis(), time0));
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
