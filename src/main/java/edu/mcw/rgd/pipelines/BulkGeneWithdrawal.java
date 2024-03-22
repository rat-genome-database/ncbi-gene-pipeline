package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.util.List;

public class BulkGeneWithdrawal {

    static public void main(String[] args) throws Exception {

        Dao dao = new Dao();

        String fname = "/tmp/rat_genes_withdrawal_2024-03-22.txt";
        BufferedReader in = Utils.openReader(fname);

        //#GeneID	RGD ID	Symbol	Name
        //120101061	41053275	Rn18sl1	18S ribosomal RNA like 1

        int invalidLines = 0; // lines with invalid GeneID or RGD ID
        int goodLines = 0; // lines with invalid GeneID or RGD ID

        String line;
        while( (line=in.readLine())!=null ) {
            String[] cols = line.split("[\\t]", -1);
            if( cols.length < 4 ) {
                continue;
            }
            String egId = cols[0];
            String rgdIdStr = cols[1];

            int rgdId = 0;
            try {
                rgdId = Integer.parseInt(rgdIdStr);
            } catch( NumberFormatException e) {}
            if( rgdId==0 ) {
                System.out.println(" RGD ID "+rgdIdStr+" not valid!");
                invalidLines++;
                continue;
            }

            RgdId id = dao.getRgdId(rgdId);
            if( id==null ) {
                System.out.println(" RGD ID "+rgdIdStr+" not in RGD!");
                invalidLines++;
                continue;
            }
            if( !id.getObjectStatus().equals("ACTIVE") ) {
                System.out.println(" RGD ID "+rgdIdStr+" not active in RGD!");
                invalidLines++;
                continue;
            }

            List<Gene> genes = dao.getGenesByEGID(egId);
            Gene gene = null;
            for( Gene g: genes ) {
                if( g.getRgdId()==rgdId ) {
                    gene = g;
                    break;
                }
            }
            if( gene==null ) {
                System.out.println(" EG ID problem: "+egId);
                invalidLines++;
                continue;
            }

            goodLines++;
            dao.withdraw(gene);
        }

        in.close();

        System.out.println(" GOOD lINES: "+goodLines);
        System.out.println(" INVALID lINES: "+invalidLines);
    }
}
