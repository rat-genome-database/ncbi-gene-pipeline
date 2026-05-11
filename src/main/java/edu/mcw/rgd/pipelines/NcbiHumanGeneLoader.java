package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads the NCBI human gene_info file, parses it, and loads new human genes into RGD.
 * Only rows with HGNC IDs are processed. For genes already in RGD (matched by HGNC xref),
 * disparities are logged to a conflict log. For genes not yet in RGD, new gene records and xrefs are created.
 */
public class NcbiHumanGeneLoader {

    Dao dao = new Dao();
    private String externalFile;

    Logger log = LogManager.getLogger("status");
    Logger logConflicts = LogManager.getLogger("human_gene_conflicts");

    public void run() throws Exception {

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();
        try {
            // download file from NCBI
            FileDownloader fd = new FileDownloader();
            fd.setExternalFile(getExternalFile());
            fd.setAppendDateStamp(true);
            fd.setLocalFile("data/ncbi_human_gene_info.gz");
            String localFile = fd.downloadNew();
            log.info("Downloaded " + getExternalFile());

            CounterPool counters = new CounterPool();

            // preload all active human HGNC xrefs and genes into in-memory maps once,
            // so per-row lookups don't hit the DB
            Map<String, Gene> hgncToGene = loadHgncToGeneMap();
            log.info("Active human genes with HGNC xrefs in RGD: " + hgncToGene.size());

            // parse all lines, filtering to those with HGNC IDs
            List<String[]> rows = parseFile(localFile, counters);

            for (String[] cols : rows) {
                try {
                    processRow(cols, hgncToGene, counters);
                } catch (Exception e) {
                    counters.increment("ROWS_WITH_ERRORS");
                    log.error("ERROR processing row", e);
                }
            }

            log.info(counters.dumpAlphabetically());
        } finally {
            memoryMonitor.stop();
            log.info(memoryMonitor.getSummary());
        }
    }

    List<String[]> parseFile(String localFile, CounterPool counters) throws Exception {

        List<String[]> rows = new ArrayList<>();

        BufferedReader in = Utils.openReader(localFile);
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }

            String[] cols = line.split("[\\t]", -1);
            if (cols.length < 12) {
                counters.increment("LINES_SKIPPED_TOO_FEW_COLUMNS");
                continue;
            }

            counters.increment("LINES_READ");

            // check if dbXrefs column (col index 5) contains an HGNC ID
            String dbXrefs = cols[5];
            String hgncId = extractHgncId(dbXrefs);
            if (hgncId == null) {
                counters.increment("LINES_SKIPPED_NO_HGNC");
                continue;
            }

            rows.add(cols);
        }
        in.close();

        log.info("Lines with HGNC IDs to be processed: " + rows.size());
        return rows;
    }

    /** one-time bulk fetch: build a map from HGNC acc_id (e.g. "HGNC:5") to its active human Gene */
    Map<String, Gene> loadHgncToGeneMap() throws Exception {
        Map<Integer, Gene> byRgdId = new HashMap<>();
        for (Gene g : dao.getActiveGenes(SpeciesType.HUMAN)) {
            byRgdId.put(g.getRgdId(), g);
        }
        Map<String, Gene> byHgnc = new HashMap<>();
        for (XdbId x : dao.getActiveXdbIds(XdbId.XDB_KEY_HGNC, SpeciesType.HUMAN)) {
            Gene g = byRgdId.get(x.getRgdId());
            if (g != null) {
                byHgnc.put(x.getAccId(), g);
            }
        }
        return byHgnc;
    }

    void processRow(String[] cols, Map<String, Gene> hgncToGene, CounterPool counters) throws Exception {

        String ncbiGeneId = cols[1];
        String symbol = cols[2];
        String dbXrefs = cols[5];
        String description = cols[8];
        String typeOfGene = cols[9];
        String symbolFromAuth = cols[10];
        String fullNameFromAuth = cols[11];

        // e.g. "HGNC:5" extracted from "HGNC:HGNC:5" in the dbXrefs column
        String hgncId = extractHgncId(dbXrefs);
        String ensemblId = extractEnsemblId(dbXrefs);

        // prefer nomenclature authority values when available
        String useSymbol = !symbolFromAuth.equals("-") ? symbolFromAuth : symbol;
        String useName = !fullNameFromAuth.equals("-") ? fullNameFromAuth : description;

        // look up existing gene in RGD by HGNC xref via preloaded map (no DB roundtrip)
        Gene existing = hgncToGene.get(hgncId);

        if (existing != null) {
            // gene already exists -- check for disparities
            counters.increment("GENES_ALREADY_IN_RGD");

            boolean hasDisparity = false;
            StringBuilder sb = new StringBuilder();
            sb.append(hgncId);
            sb.append("  RGD:").append(existing.getRgdId());
            sb.append("  NCBI:").append(ncbiGeneId);

            if (!Utils.stringsAreEqual(existing.getSymbol(), useSymbol)) {
                sb.append("  SYMBOL: rgd=").append(existing.getSymbol()).append(" ncbi=").append(useSymbol);
                hasDisparity = true;
            }
            if (!Utils.stringsAreEqual(existing.getName(), useName)) {
                sb.append("  NAME: rgd=").append(existing.getName()).append(" ncbi=").append(useName);
                hasDisparity = true;
            }
            if (!Utils.stringsAreEqual(existing.getDescription(), description)) {
                sb.append("  DESC: rgd=").append(existing.getDescription()).append(" ncbi=").append(description);
                hasDisparity = true;
            }

            if (hasDisparity) {
                logConflicts.debug(sb.toString());
                counters.increment("GENES_WITH_DISPARITIES");
            }
        } else {
            // gene not in RGD -- create new gene
            counters.increment("GENES_CREATED");

            RgdId rgdId = dao.createRgdId(RgdId.OBJECT_KEY_GENES, "ACTIVE",
                    "Created by NcbiHumanGeneLoader", SpeciesType.HUMAN);
            int newRgdId = rgdId.getRgdId();

            Gene gene = new Gene();
            gene.setRgdId(newRgdId);
            gene.setSymbol(useSymbol);
            gene.setName(useName);
            gene.setDescription(description);
            gene.setType(typeOfGene);
            gene.setGeneSource("NCBI");
            gene.setSpeciesTypeKey(SpeciesType.HUMAN);
            dao.insertGene(gene);

            // insert xrefs
            List<XdbId> xdbIds = new ArrayList<>();

            // HGNC xref -- accId is "HGNC:xxx"
            XdbId hgncXdb = new XdbId();
            hgncXdb.setRgdId(newRgdId);
            hgncXdb.setXdbKey(XdbId.XDB_KEY_HGNC);
            hgncXdb.setAccId(hgncId);
            hgncXdb.setSrcPipeline("NcbiHumanGeneLoader");
            xdbIds.add(hgncXdb);

            // NCBI Gene xref
            XdbId ncbiXdb = new XdbId();
            ncbiXdb.setRgdId(newRgdId);
            ncbiXdb.setXdbKey(XdbId.XDB_KEY_NCBI_GENE);
            ncbiXdb.setAccId(ncbiGeneId);
            ncbiXdb.setSrcPipeline("NcbiHumanGeneLoader");
            xdbIds.add(ncbiXdb);

            // Ensembl xref (if present)
            if (ensemblId != null) {
                XdbId ensemblXdb = new XdbId();
                ensemblXdb.setRgdId(newRgdId);
                ensemblXdb.setXdbKey(XdbId.XDB_KEY_ENSEMBL_GENES);
                ensemblXdb.setAccId(ensemblId);
                ensemblXdb.setSrcPipeline("NcbiHumanGeneLoader");
                xdbIds.add(ensemblXdb);
            }

            dao.insertXdbs(xdbIds, "GENE");

            log.info("Created gene RGD:" + newRgdId + " " + useSymbol + " " + hgncId);
        }
    }

    /**
     * Extract HGNC ID from dbXrefs column.
     * The dbXrefs format is pipe-separated: "HGNC:HGNC:5|Ensembl:ENSG00000121410|MIM:138670"
     * The first "HGNC:" is the source prefix; "HGNC:5" is the actual accession.
     * Returns "HGNC:5" or null if not found.
     */
    String extractHgncId(String dbXrefs) {
        if (dbXrefs == null || dbXrefs.equals("-")) {
            return null;
        }
        for (String xref : dbXrefs.split("\\|")) {
            if (xref.startsWith("HGNC:HGNC:")) {
                // strip the source prefix "HGNC:" to get "HGNC:5"
                return xref.substring("HGNC:".length());
            }
        }
        return null;
    }

    /**
     * Extract Ensembl gene ID from dbXrefs column.
     * Returns the accession (e.g. "ENSG00000121410") or null if not found.
     */
    String extractEnsemblId(String dbXrefs) {
        if (dbXrefs == null || dbXrefs.equals("-")) {
            return null;
        }
        for (String xref : dbXrefs.split("\\|")) {
            if (xref.startsWith("Ensembl:")) {
                return xref.substring("Ensembl:".length());
            }
        }
        return null;
    }

    public void setExternalFile(String externalFile) {
        this.externalFile = externalFile;
    }

    public String getExternalFile() {
        return externalFile;
    }
}
