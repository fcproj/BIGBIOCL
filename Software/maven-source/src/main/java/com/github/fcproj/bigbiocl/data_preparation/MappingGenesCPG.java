package com.github.fcproj.bigbiocl.data_preparation;

import java.util.Map;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Write a CSV (comma separated) mapping CpGs to Genes.
 * Input:
 * - BED file, where cpg has index 4 and gene has index 6
 * - output CSV path
 * @author fabrizio
 *
 */
public class MappingGenesCPG {
	
	public static void main(String[] args) throws Exception{
		//String mappingFilePath = "D:/Experiments/DNAMeth_brca_FULL/mapping/TCGA-07-0227-20A-01D-A32T-05.bed";
		//String outputFilePath = "D:/Experiments/DNAMeth_brca_FULL/mapping/cpg2genes.csv";
		
		if(args.length<2)
			throw new Exception("Expected: BED file where cpg has index 4 and gene has index 6; output CSV path");
		
		String mappingFilePath = args[0];
		String outputFilePath = args[1];
		
		FileManager ioManager = new FileManager();
		Map<String, String> cpg2gene = ioManager.parseGenes2CpGBED(mappingFilePath);
		ioManager.writeMapString(cpg2gene, outputFilePath);
	}

}
