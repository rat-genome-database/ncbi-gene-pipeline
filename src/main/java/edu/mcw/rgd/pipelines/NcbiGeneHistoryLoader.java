package edu.mcw.rgd.pipelines;


import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.util.*;

/**
 * @author mtutaj
 * @since Feb 11, 2019
 * handle NCBI file with withdrawn and retired genes
 */
public class NcbiGeneHistoryLoader {

    Dao dao = new Dao();
    private String externalFile;

    public void run() throws Exception {

        // download gene history file from NCBI
        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(getExternalFile());
        fd.setAppendDateStamp(true);
        fd.setLocalFile("data/ncbi_gene_history.gz");
        String localFile = fd.downloadNew();
        System.out.println("Downloaded "+getExternalFile());

        Counters counters = new Counters();

        Set<String> taxons = new HashSet<>();
        for( int skey: SpeciesType.getSpeciesTypeKeys() ) {
            if( skey!=SpeciesType.RAT ) {
                taxons.add(Integer.toString(SpeciesType.getTaxonomicId(skey)));
            }
        }

        List<String> lines = new ArrayList<>();
        // read the header:
        // #tax_id	GeneID	Discontinued_GeneID	Discontinued_Symbol	Discontinue_Date
        BufferedReader in = Utils.openReader(localFile);
        String line;
        while( (line=in.readLine())!=null ) {
            // skip comment lines
            if (line.startsWith("#")) {
                continue;
            }

            int tabPos = line.indexOf('\t');
            if( tabPos<0 ) {
                continue;
            }
            String taxon = line.substring(0, tabPos);
            if( !taxons.contains(taxon) ) {
                counters.increment("Lines skipped - unsupported taxon");
                continue;
            }
            lines.add(line);
        }
        in.close();
        Collections.shuffle(lines);
        System.out.println("Lines to be processed "+lines.size());

        // process lines
        for( String l: lines ) {
            line = l;
            // there must be at least 5 columns available
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<5 ) {
                continue;
            }

            String oldGeneId = cols[2]; // id of discontinued gene

            String newGeneId = cols[1]; // only set if discontinued gene was replaced by another gene

            List<Gene> oldGenes = dao.getActiveGenesByXdbId(XdbId.XDB_KEY_NCBI_GENE, oldGeneId);
            if( oldGenes.isEmpty() ) {
                counters.increment("Lines skipped -- already inactive in RGD");
                continue;
            }
            Gene oldGene = oldGenes.get(0);

            // skip rat genes
            if( oldGene.getSpeciesTypeKey()==SpeciesType.RAT ) {
                counters.increment("Lines skipped -- rat genes");
                continue;
            }

            Gene newGene = null;
            if( !newGeneId.equals("-") ) {
                List<Gene> newGenes = dao.getActiveGenesByXdbId(XdbId.XDB_KEY_NCBI_GENE, newGeneId);
                if( newGenes.size()>0 ) {
                    newGene = newGenes.get(0);
                }
            }

            // handle simple withdrawals
            String species = SpeciesType.getCommonName(oldGene.getSpeciesTypeKey());
            if( newGene==null ) {
                counters.increment("GENES PROCESSED");
                int cnt = counters.get("GENES PROCESSED");
                if( newGeneId.equals("-") ) {
                    dao.withdraw(oldGene);
                    System.out.println(cnt+". WITHDRAW: " + line);
                    counters.increment("GENES WITHDRAWN FOR " + species);
                    continue;
                } else {
                    System.out.println(cnt+". CONFLICT: " + line);
                    counters.increment("GENES WITH CONFLICT FOR " + species);
                    continue;
                }
            }

            // retire genes
            BulkGeneMerge.simpleMerge(oldGene, newGene, null, counters);
            System.out.println("  RETIRE: "+line);
            counters.increment("GENES RETIRED FOR "+species);
        }

        System.out.println("===");
        counters.dump();
    }

    public void setExternalFile(String externalFile) {
        this.externalFile = externalFile;
    }

    public String getExternalFile() {
        return externalFile;
    }
}
