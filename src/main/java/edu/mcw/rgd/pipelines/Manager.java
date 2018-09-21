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
        try {
            manager.run((SeqLoader)(bf.getBean("seqLoader")));
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

        final int insertedSeqCap = 1000;
        List<Integer> speciesTypeKeys = new ArrayList(SpeciesType.getSpeciesTypeKeys());
        while( !speciesTypeKeys.isEmpty() ) {
            logStatus.info("");
            Collections.shuffle(speciesTypeKeys);
            logStatus.info("==== SPECIES TO PROCESS: "+ Arrays.toString(speciesTypeKeys.toArray()));

            String progress = "     PROGRESS: ";
            for (int sp : speciesTypeKeys) {
                String speciesProgress = seqLoader.progressMap.get(sp);
                progress += speciesProgress + ", ";
            }
            logStatus.info(progress);

            int speciesTypeKey = speciesTypeKeys.get(0);
            if( speciesTypeKey == SpeciesType.ALL ) {
                speciesTypeKeys.remove(0);
                continue;
            }

            int seqsInserted = seqLoader.run(speciesTypeKey, insertedSeqCap);
            // if all sequences for this species are done, remove it from the processing list
            if (seqsInserted <= 0) {
                speciesTypeKeys.remove(0);
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
