package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.Alias;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.NomenclatureEvent;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class BulkGeneRename {

    public static void main(String[] args) throws Exception {

        try {
            int speciesTypeKey = 3;
            String fname = "/tmp/rat_nomen.txt";
            bulkRename(fname, speciesTypeKey);
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    static void bulkRename(String fname, int speciesTypeKey) throws Exception {

        CounterPool counters = new CounterPool();
        Dao dao = new Dao();
        HashSet<Integer> rgdIdSet = new HashSet<>();
        HashSet<Integer> duplicates = new HashSet<>();

        // file is a TAB-separated file with the following columns:
        // #RGDID	New name	New symbol
        //1586626	alpha- and gamma-adaptin binding protein, pseudogene 1	Aagab-ps1
        BufferedReader in = Utils.openReader(fname);
        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            if( line.startsWith("#") ) {
                continue;
            }
            String[] cols = line.split("[\\t]", -1);
            String rgdIdStr = cols[0];
            if( Utils.isStringEmpty(rgdIdStr) ) {
                counters.increment("NO RGD ID for line "+lineNr+": "+line);
                continue;
            }
            int rgdId = Integer.parseInt(rgdIdStr);
            if( !rgdIdSet.add(rgdId) ) {
                System.out.println(lineNr+". DUPLICATE: "+line);
                duplicates.add(rgdId);
            }
            String newName = cols[1].trim();
            String newSymbol = cols[2].trim();
            if( !simpleRename(rgdId, newName, newSymbol, counters, dao, speciesTypeKey) ) {
                System.out.println(lineNr+". SKIPPED: "+line);
            }
        }
        in.close();

        System.out.println(counters.dumpAlphabetically());

        System.out.println("DUPLICATES: "+duplicates.size());
        for( int id: duplicates ) {
            System.out.println("RGD:"+id);
        }
    }

    static boolean simpleRename(int rgdId, String newName, String newSymbol, CounterPool counters, Dao dao, int speciesTypeKey) throws Exception {

        counters.increment("GENES PROCESSED");

        // both rgd ids must be active
        RgdId id = dao.getRgdId(rgdId);
        if( !id.getObjectStatus().equals("ACTIVE") || id.getSpeciesTypeKey()!= speciesTypeKey ) {
            System.out.println(rgdId+" is NOT an active gene or is wrong species!");
            counters.increment("genes skipped");
            return false;
        }

        Gene gene = dao.getGene(rgdId);

        String oldSymbol = gene.getSymbol();
        String oldName = gene.getName();

        boolean symbolChanged = !Utils.stringsAreEqual(oldSymbol, newSymbol);
        boolean nameChanged = !Utils.stringsAreEqual(oldName, newName);

        if( !symbolChanged && !nameChanged ) {
            System.out.println(rgdId+" no name or symbol changed!");
            counters.increment("genes skipped");
            return false;
        }

        // update gene
        gene.setSymbol(newSymbol);
        gene.setName(newName);
        dao.updateGene(gene);
        dao.updateLastModifiedDate(rgdId);

        int aliasesInserted = 0;
        if( symbolChanged ) {
            aliasesInserted += addAlias("old_gene_symbol", oldSymbol, rgdId, dao);
        }
        if( nameChanged ) {
            aliasesInserted += addAlias("old_gene_name", oldName, rgdId, dao);
        }

        // create nomenclature event
        NomenclatureEvent ev = new NomenclatureEvent();

        String nomenDesc = "";
        if( symbolChanged && nameChanged ) {
            nomenDesc = "Symbol and Name Changed";
        }
        else if( symbolChanged && !nameChanged ) {
            nomenDesc = "Symbol Changed";
        }
        else if( !symbolChanged && nameChanged ) {
            nomenDesc = "Name Changed";
        }

        ev.setDesc(nomenDesc);
        ev.setEventDate(new Date());
        ev.setName(newName);
        ev.setNomenStatusType("APPROVED");
        ev.setNotes("internal nomenclature review of pseudogenes");
        ev.setOriginalRGDId(rgdId);
        ev.setPreviousName(oldName);
        ev.setPreviousSymbol(oldSymbol);
        ev.setRefKey("10779");
        ev.setRgdId(rgdId);
        ev.setSymbol(newSymbol);
        dao.createNomenEvent(ev);

        System.out.println((counters.get("GENES PROCESSED")+1)
                +".  RGD:"+rgdId+"\n"
                +"   SYMBOL ["+oldSymbol+"] ==> ["+newSymbol+"]\n"
                +"   SYMBOL ["+oldName+"] ==> ["+newName+"]\n");

        counters.add("aliases inserted", aliasesInserted);
        counters.increment("nomen inserted");
        return true;
    }

    static int addAlias(String aliasType, String aliasValue, int rgdId, Dao dao) throws Exception {

        if( Utils.isStringEmpty(aliasValue) ) {
            return 0;
        }

        // is the alias already in rgd?
        List<Alias> aliasesInRgd = dao.getAliases(rgdId, aliasType);
        for( Alias a: aliasesInRgd ) {
            if( Utils.stringsAreEqualIgnoreCase(a.getValue(), aliasValue) ) {
                return 0; // yes, it is -- do nothing (don't insert duplicates)
            }
        }

        Alias alias = new Alias();
        alias.setTypeName(aliasType);
        alias.setValue(aliasValue);
        alias.setRgdId(rgdId);
        alias.setNotes("created by GeneMerge tool on " + new Date());
        dao.insertAlias(alias);
        return 1;
    }

}
