package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.List;

public class FastaCleanup {

    public static void main(String[] args) throws Exception {

        //int mapKey = 17;
        //String name1 = "data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz";
        //String name2 = "data/GRCh37.p13.fa.gz";
        int mapKey = 634;
        String name1 = "data/GCF_014441545.1_ROS_Cfam_1.0_genomic.fna.gz";
        String name2 = "data/ROS_Cfam_1.0.fa.gz";


        MapDAO mapDAO = new MapDAO();
        List<Chromosome> chromosomes = mapDAO.getChromosomes(mapKey);

        BufferedReader in = Utils.openReader(name1);
        BufferedWriter out = Utils.openWriter(name2);

        String line;
        boolean isNC = false;
        while( (line=in.readLine())!=null ) {
            if( line.startsWith(">") ) {
                isNC = line.startsWith(">NC");

                if( isNC ) {
                    // find chromosome replacement
                    int pos = line.indexOf(' ');
                    String chrAcc = line.substring(1, pos);
                    String chr = null;
                    for( Chromosome c: chromosomes ) {
                        if( c.getRefseqId().equals(chrAcc) ) {
                            chr = "Chr"+c.getChromosome();
                            break;
                        }
                    }

                    String header = line;
                    if( chr!=null ) {
                        header = ">"+chr+" - "+line.substring(1);
                    }
                    out.write(header);
                    out.write("\n");

                    System.out.println(header);
                    continue;
                }
            }
            if( isNC ) {
                out.write(line);
                out.write("\n");
            }
        }
        in.close();
        out.close();

        System.out.println("DONE");
    }
}
