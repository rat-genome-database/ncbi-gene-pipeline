package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.Transcript;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TranscriptQC {

    Logger log = LogManager.getLogger("transcriptQC");

    public void run() throws Exception {

        Dao dao = new Dao();

        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("TRANSCRIPT QC   started at "+sdt.format(new Date()));
        log.info("===");

        List<Integer> speciesTypeKeys = new ArrayList<>(SpeciesType.getSpeciesTypeKeys());
        speciesTypeKeys.removeIf( sp -> !SpeciesType.isSearchable(sp) );
        Collections.shuffle(speciesTypeKeys);

        for( int speciesTypeKey: speciesTypeKeys ) {

            log.info(SpeciesType.getCommonName(speciesTypeKey).toUpperCase());

            AtomicInteger activeTranscripts = new AtomicInteger(0);
            AtomicInteger withdrawnTranscripts1 = new AtomicInteger(0);
            AtomicInteger withdrawnTranscripts2 = new AtomicInteger(0);

            List<RgdId> trIds = dao.getRgdIds( RgdId.OBJECT_KEY_TRANSCRIPTS, speciesTypeKey );

            trIds.parallelStream().forEach( trId -> {

                if (trId.getObjectStatus().equals("ACTIVE")) {
                    activeTranscripts.incrementAndGet();

                    try {
                        Transcript tr = dao.getTranscript(trId.getRgdId());
                        if (tr == null) {
                            dao.withdraw(trId);
                            withdrawnTranscripts1.incrementAndGet();

                        } else {
                            RgdId geneId = dao.getRgdId(tr.getGeneRgdId());
                            if (!geneId.getObjectStatus().equals("ACTIVE")) {
                                dao.withdraw(trId);
                                int cnt = withdrawnTranscripts2.incrementAndGet();

                                log.info("   " + cnt + ". " + tr.getAccId() + " RGD:" + trId.getRgdId() + "   gene RGD:" + tr.getGeneRgdId());
                            }
                        }
                    } catch( Exception e ) {
                        throw new RuntimeException(e);
                    }
                }
            });

            log.info("   all transcripts: "+trIds.size());
            log.info("   active transcripts processed: "+activeTranscripts);
            log.info("   withdrawn transcripts (no entry in TRANSCRIPTS table): "+withdrawnTranscripts1);
            log.info("   withdrawn transcripts (associated with inactive gene): "+withdrawnTranscripts2);
            log.info("");
        }

        log.info("===");
        log.info("");
    }
}
