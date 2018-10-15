package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
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

    Logger logStatus = Logger.getLogger("status");

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Manager manager = (Manager) (bf.getBean("manager"));

        boolean loadRnaSeqs = false;
        boolean loadProteinSeqs = false;

        for( String arg: args ) {
           switch(arg) {
               case "--load_rna_seqs":
                   loadRnaSeqs = true;
                   break;
               case "--load_protein_seqs":
                   loadProteinSeqs = true;
                   break;
               default:
                   System.out.println("WARN: unknown cmdline parameter");
           }
        }

        try {
            if( loadRnaSeqs ) {
                SeqLoader seqLoader = (SeqLoader) (bf.getBean("rnaSeqLoader"));
                manager.run(seqLoader);
            }
            if( loadProteinSeqs ) {
                SeqLoader seqLoader = (SeqLoader) (bf.getBean("proteinSeqLoader"));
                manager.run(seqLoader);
            }
        } catch(Exception e) {
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
