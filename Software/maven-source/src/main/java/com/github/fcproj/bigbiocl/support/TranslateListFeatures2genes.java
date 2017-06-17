package com.github.fcproj.bigbiocl.support;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Produce a CSV file mapping: MLlib feature name , original feature name (e.g. cpg island) , gene name
 * 
 * Input:
 * - CSV file (comma separated) mapping original feature names (e.g. cpg island) to gene names
 * - CSV file (comma separated) mapping MLlib feature names to original feature names (e.g. cpg island)
 * - TXT file containing the list of MLlib features, one per line (e.g. obtained using the LoadModel application)
 * - separator for the output CSV file (e.g. ; or ,)
 * 
 * Output:
 * CSV file mapping: MLlib feature name , original feature name (e.g. cpg island) , gene name
 * 
 * @author fabrizio
 *
 */
public class TranslateListFeatures2genes {

	public static void main(String[] args) throws Exception{
		//String cpg2genesPath = "D:/Experiments/DNAMeth_brca_cpg/mapping/cpg2genes.csv";
		//String features2cpgPath = "D:/Experiments/DNAMeth_brca_cpg/mapping/features2cpg.csv";
		//String featuresPath = "D:/Experiments/DNAMeth_brca_cpg/18_32/features.txt";
		//String outputPath = "D:/Experiments/DNAMeth_brca_cpg/18_32/mergedGenes.csv";
		
		if(args.length<5)
			throw new Exception("Expected: cpg2genes Path, features2cpg Path, feature Path, output Path, separator");
		
		String cpg2genesPath = args[0];
		String features2cpgPath = args[1];
		String featuresPath = args[2];
		String outputPath = args[3];
		
		String separator = args[4];

		FileManager ioManager = new FileManager();
		Map<String, String> cpg2genes = ioManager.parseCommaTxt(cpg2genesPath);
		Map<String, String> features2cpg = ioManager.parseCommaTxt(features2cpgPath);
		Set<String> features = ioManager.parseLines(featuresPath);
		List<String> result = new LinkedList<String>();	
		
		//build the CSV file
		for(String feature: features){
			String cpg = features2cpg.get(feature);
			if(cpg!=null){
				result.add(feature+separator+cpg+separator+cpg2genes.get(cpg));
			}
		}
		
		ioManager.writeLines(result, outputPath);
	}

}
