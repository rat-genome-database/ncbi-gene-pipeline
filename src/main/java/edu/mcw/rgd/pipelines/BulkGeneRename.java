package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.util.*;
import java.util.Map;

public class BulkGeneRename {

    static Logger logStatus = LogManager.getLogger("status");

    public static void main(String[] args) throws Exception {

        try {
            boolean dryRun = true;
            int speciesTypeKey = 3;
            String nomenInfo = "NCBI nomenclature review";
            String fname;

            fname = "/tmp/rat_rename.txt";
            bulkRename3(fname, speciesTypeKey, dryRun, nomenInfo);


            //String fname = "/tmp/rat_nomen3.txt";
            //bulkRename(fname, speciesTypeKey);

            //String fname = "/tmp/rat_rename_1-9-23.txt";
            //bulkRename1(fname, speciesTypeKey, dryRun, nomenInfo);

            //String fname = "/tmp/rat_olfactory_nomen3.txt";
            //bulkRename2(fname, speciesTypeKey, dryRun, nomenInfo);

            //String fname = "/tmp/rat_trna_rename.txt";
            //bulkRename3(fname, speciesTypeKey, dryRun, nomenInfo);

        } catch(Exception e) {
            Utils.printStackTrace(e, logStatus);
        }

    }

    // file is a TAB-separated file with the following columns:
    // #RGDID	New name	New symbol
    //1586626	alpha- and gamma-adaptin binding protein, pseudogene 1	Aagab-ps1
    static void bulkRename(String fname, int speciesTypeKey, boolean dryRun, String nomenInfo) throws Exception {

        CounterPool counters = new CounterPool();
        Dao dao = new Dao();
        System.out.println(dao.getConnectionInfo());

        HashSet<Integer> rgdIdSet = new HashSet<>();
        HashSet<Integer> duplicates = new HashSet<>();
        Map<Integer,List<String>> lineMap = new HashMap<>();

        BufferedReader in = Utils.openReader(fname);
        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            if( line.startsWith("#") ) {
                continue;
            }

            line = removeDoubleQuotes(line);

            String[] cols = line.split("[\\t]", -1);
            String rgdIdStr = cols[0];
            if( Utils.isStringEmpty(rgdIdStr) ) {
                counters.increment("NO RGD ID for line "+lineNr+": "+line);
                continue;
            }
            int rgdId = Integer.parseInt(rgdIdStr);

            List<String> lines = lineMap.get(rgdId);
            if( lines==null ) {
                lines = new ArrayList<>();
                lineMap.put(rgdId, lines);
            }
            lines.add(lineNr+". "+line);

            if( !rgdIdSet.add(rgdId) ) {
                System.out.println(lineNr+". DUPLICATE: "+line);
                duplicates.add(rgdId);
            }
            String newName = cols[1].trim();
            String newSymbol = cols[2].trim();
            if( !simpleRename(rgdId, newName, newSymbol, counters, dao, speciesTypeKey, dryRun, nomenInfo) ) {
                System.out.println(lineNr+". SKIPPED: "+line);
            }
        }
        in.close();

        System.out.println(counters.dumpAlphabetically());

        dumpDuplicates(duplicates, lineMap);
    }

    // file is a TAB-separated file with the following columns:
    //#RGD ID	Symbol	Name
    //1565033	C10h17orf58	similar to human chromosome 17 open reading frame 58
    static void bulkRename1(String fname, int speciesTypeKey, boolean dryRun, String nomenInfo) throws Exception {

        CounterPool counters = new CounterPool();
        Dao dao = new Dao();
        System.out.println(dao.getConnectionInfo());

        HashSet<Integer> rgdIdSet = new HashSet<>();
        HashSet<Integer> duplicates = new HashSet<>();
        Map<Integer,List<String>> lineMap = new HashMap<>();

        BufferedReader in = Utils.openReader(fname);
        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            if( line.startsWith("#") ) {
                continue;
            }

            line = removeDoubleQuotes(line);

            String[] cols = line.split("[\\t]", -1);
            String rgdIdStr = cols[0];
            if( Utils.isStringEmpty(rgdIdStr) ) {
                counters.increment("NO RGD ID for line "+lineNr+": "+line);
                continue;
            }
            int rgdId = Integer.parseInt(rgdIdStr);

            List<String> lines = lineMap.get(rgdId);
            if( lines==null ) {
                lines = new ArrayList<>();
                lineMap.put(rgdId, lines);
            }
            lines.add(lineNr+". "+line);

            if( !rgdIdSet.add(rgdId) ) {
                System.out.println(lineNr+". DUPLICATE: "+line);
                duplicates.add(rgdId);
            }
            String newName = cols[2].trim();
            String newSymbol = cols[1].trim();
            if( !simpleRename(rgdId, newName, newSymbol, counters, dao, speciesTypeKey, dryRun, nomenInfo) ) {
                System.out.println(lineNr+". SKIPPED: "+line);
            }
        }
        in.close();

        System.out.println(counters.dumpAlphabetically());

        dumpDuplicates(duplicates, lineMap);
    }

    // file is a TAB-separated file with the following columns:
    //Current symbol	New Symbol	New name
    //LOC100360557	Or13a19	olfactory receptor family 13 subfamily A member 19
    static void bulkRename2(String fname, int speciesTypeKey, boolean dryRun, String nomenInfo) throws Exception {

        CounterPool counters = new CounterPool();
        Dao dao = new Dao();
        HashSet<Integer> rgdIdSet = new HashSet<>();
        HashSet<Integer> duplicates = new HashSet<>();
        Map<Integer,List<String>> lineMap = new HashMap<>();

        BufferedReader in = Utils.openReader(fname);
        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            if( line.startsWith("#") ) {
                continue;
            }

            line = removeDoubleQuotes(line);

            String[] cols = line.split("[\\t]", -1);
            String oldSymbol = cols[0];
            if( Utils.isStringEmpty(oldSymbol) ) {
                counters.increment("NO OLD SYMBOL for line "+lineNr+": "+line);
                continue;
            }
            List<Gene> matchingGenes = dao.getGenesBySymbolAndSpecies(oldSymbol, SpeciesType.RAT);
            if( matchingGenes.isEmpty() ) {
                counters.increment("NO GENE MATCH BY SYMBOL for line "+lineNr+": "+line);
                continue;
            }
            if( matchingGenes.size()>1 ) {
                // remove inactive genes
                for( int i=matchingGenes.size()-1; i>=0; i-- ) {
                    Gene g = matchingGenes.get(i);
                    RgdId id = dao.getRgdId(g.getRgdId());
                    if( !id.getObjectStatus().equals("ACTIVE") ) {
                        matchingGenes.remove(i);
                    }
                }

                if( matchingGenes.size()>1 ) {
                    counters.increment("MULTI GENE MATCH BY SYMBOL for line " + lineNr + ": " + line);
                    continue;
                }
            }
            int rgdId = matchingGenes.get(0).getRgdId();

            List<String> lines = lineMap.get(rgdId);
            if( lines==null ) {
                lines = new ArrayList<>();
                lineMap.put(rgdId, lines);
            }
            lines.add(lineNr+". "+line);

            if( !rgdIdSet.add(rgdId) ) {
                System.out.println(lineNr+". DUPLICATE: "+line);
                duplicates.add(rgdId);
            }
            String newSymbol = cols[1].trim();
            String newName = cols[2].trim();
            if( !simpleRename(rgdId, newName, newSymbol, counters, dao, speciesTypeKey, dryRun, nomenInfo) ) {
                System.out.println(lineNr+". SKIPPED: "+line);
            }
        }
        in.close();

        System.out.println(counters.dumpAlphabetically());

        dumpDuplicates(duplicates, lineMap);
    }

    // file is a TAB-separated file with the following columns:
    //    RGDID	Current symbol	New name	New Symbol
    //1582902	LOC690206	RIKEN cDNA 1700013G24 gene like	1700013G24Rikl
    static void bulkRename3(String fname, int speciesTypeKey, boolean dryRun, String nomenInfo) throws Exception {

        boolean onlyQC = true; // re-run -- just run the basic qc

        CounterPool counters = new CounterPool();
        Dao dao = new Dao();
        HashSet<Integer> rgdIdSet = new HashSet<>();
        HashSet<Integer> duplicates = new HashSet<>();
        Map<Integer,List<String>> lineMap = new HashMap<>();

        BufferedReader in = Utils.openReader(fname);
        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            if( line.startsWith("#") ) {
                continue;
            }

            line = removeDoubleQuotes(line);

            String[] cols = line.split("[\\t]", -1);
            String rgdIdStr = cols[0].trim();
            String oldSymbol = cols[1].trim();
            String newName = cols[2].trim();
            String newSymbol = cols[3].trim();

            if( Utils.isStringEmpty(rgdIdStr) ) {
                counters.increment("NO RGD_ID for line "+lineNr+": "+line);
                continue;
            }

            int rgdId = 0;
            try {
                rgdId = Integer.parseInt(rgdIdStr);
            } catch (Exception ignore) {}
            if( rgdId==0 ) {
                counters.increment("NO RGD_ID for line "+lineNr+": "+line);
                continue;
            }

            Gene gene = dao.getGene(rgdId);
            if( gene==null ) {
                counters.increment("cannot find gene with RGD ID "+rgdId);
                continue;
            }

            if( Utils.isStringEmpty(oldSymbol) ) {
                counters.increment("NO OLD SYMBOL for line "+lineNr+": "+line);
                continue;
            }

            if( gene.getSymbol().equals(newSymbol) ) {
                if( gene.getName().equals(newName) ) {
                    counters.increment("SYMBOL and NAME already up-to-date");
                } else {
                    counters.increment("SYMBOL already up-to-date, NAME is not");
                    System.out.println(lineNr+". NAME conflict! (symbols match)   RGD:"+rgdIdStr+"  in RGD=["+gene.getName()+"] requested=["+newName+"]");
                }

                List<Gene> genesMatchingBySymbol = dao.getGenesBySymbolAndSpecies(newSymbol, speciesTypeKey);
                if( genesMatchingBySymbol.size()!=1 || genesMatchingBySymbol.get(0).getRgdId() != rgdId ) {
                    String msg = lineNr+". SYMBOL MULTIS: ";
                    for( Gene gg: genesMatchingBySymbol ) {
                        msg += "   RGD:"+gg.getRgdId()+" "+gg.getSymbol();
                    }
                    System.out.println(msg);
                }
                continue;
            }

            RgdId id = dao.getRgdId(rgdId);
            if( !id.getObjectStatus().equals("ACTIVE")  ) {
                counters.increment("INACTIVE genes");
                System.out.println(lineNr
                        +".  INACTIVE GENE!    RGD:"+rgdId+" ["+newSymbol+"] ["+newName+"]");
                continue;
            }

            if( onlyQC ) {
                // just dump symbol differences
                System.out.println(lineNr+". SYMBOL mismatch!   RGD:" + rgdIdStr
                        + " in RGD:[" + gene.getSymbol() + "]"
                        + " in file:[" + newSymbol + "]");
            }
            else {
                if (!gene.getSymbol().equals(oldSymbol)) {
                    counters.increment("SYMBOL doesn't match the input file; line " + lineNr + " RGD:" + rgdIdStr
                            + " in-file:[" + oldSymbol + "]  in db:[" + gene.getSymbol() + "]"
                            + " new symbol:[" + newSymbol + "]");
                    continue;
                }

                List<String> lines = lineMap.get(rgdId);
                if (lines == null) {
                    lines = new ArrayList<>();
                    lineMap.put(rgdId, lines);
                }
                lines.add(lineNr + ". " + line);

                if (!rgdIdSet.add(rgdId)) {
                    System.out.println(lineNr + ". DUPLICATE: " + line);
                    duplicates.add(rgdId);
                }
                if (!simpleRename(rgdId, newName, newSymbol, counters, dao, speciesTypeKey, dryRun, nomenInfo)) {
                    System.out.println(lineNr + ". SKIPPED: " + line);
                }
            }
        }
        in.close();

        System.out.println(counters.dumpAlphabetically());

        dumpDuplicates(duplicates, lineMap);
    }

    static void dumpDuplicates( Set<Integer> duplicates, Map<Integer,List<String>> lineMap ) {

        System.out.println("DUPLICATES: "+duplicates.size());
        for( int id: duplicates ) {
            System.out.println("RGD:"+id);
            List<String> lines = lineMap.get(id);
            for( String aLine: lines ) {
                System.out.println("    "+aLine);
            }
        }
    }

    static String removeDoubleQuotes(String s) {

        while( true) {
            int pos1 = s.indexOf('\"');
            if( pos1<0 ) {
                break; // no double quotes in 's'
            }
            int pos2 = s.indexOf('\"', pos1+1);
            if( pos2<0 ) {
                break; // no matching double quotes
            }

            String out = "";
            if( pos1>0 ) {
                out = s.substring(0, pos1);
            }
            String quotedStr = s.substring(pos1+1, pos2).trim();
            out += quotedStr;

            out += s.substring(pos2+1);
            s = out;
        }
        return s;
    }

    static boolean simpleRename(int rgdId, String newName, String newSymbol, CounterPool counters, Dao dao, int speciesTypeKey, boolean dryRun,
                                String nomenInfo) throws Exception {

        counters.increment("GENES PROCESSED");

        // rgd id must be active
        RgdId id = dao.getRgdId(rgdId);
        if( !id.getObjectStatus().equals("ACTIVE")  ) {
            counters.increment((counters.get("GENES PROCESSED")+1)
                    +".  INACTIVE GENE!    RGD:"+rgdId+" ["+newSymbol+"] ["+newName+"]");
            return false;
        }
        if( id.getSpeciesTypeKey()!= speciesTypeKey ) {
            counters.increment((counters.get("GENES PROCESSED")+1)
                    +".  WRONG SPECIES!    RGD:"+rgdId+" ["+newSymbol+"] ["+newName+"]");
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
        if( !dryRun ) {
            dao.updateGene(gene);
            dao.updateLastModifiedDate(rgdId);
        }
        counters.increment("genes renamed");

        int aliasesInserted = 0;
        if( symbolChanged ) {
            aliasesInserted += addAlias("old_gene_symbol", oldSymbol, rgdId, dao, dryRun);
        }
        if( nameChanged ) {
            aliasesInserted += addAlias("old_gene_name", oldName, rgdId, dao, dryRun);
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
        ev.setNotes(nomenInfo);
        ev.setOriginalRGDId(rgdId);
        ev.setPreviousName(oldName);
        ev.setPreviousSymbol(oldSymbol);
        ev.setRefKey("10779");
        ev.setRgdId(rgdId);
        ev.setSymbol(newSymbol);
        if( !dryRun ) {
            dao.createNomenEvent(ev);
        }

        System.out.println((counters.get("GENES PROCESSED")+1)
                +".  RGD:"+rgdId+"\n"
                +"   SYMBOL ["+oldSymbol+"] ==> ["+newSymbol+"]\n"
                +"   NAME ["+oldName+"] ==> ["+newName+"]\n");

        counters.add("aliases inserted", aliasesInserted);
        counters.increment("nomen inserted");
        return true;
    }

    static int addAlias(String aliasType, String aliasValue, int rgdId, Dao dao, boolean dryRun) throws Exception {

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
        alias.setNotes("created by BulkGeneRename tool on " + new Date());
        if( !dryRun ) {
            dao.insertAlias(alias);
        }
        return 1;
    }
}
