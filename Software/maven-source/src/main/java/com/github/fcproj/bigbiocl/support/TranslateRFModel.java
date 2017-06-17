package com.github.fcproj.bigbiocl.support;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.fcproj.bigbiocl.util.DirectoryManager;
import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Given the MLlib Random Forest debug string and a mapping file Spark_Features , Experiment_Features,
 * extracts the list of CpG dinuclotides from a Spark random forest debug string. 
 * 
 * It requires 3 command line arguments:
 * - a CSV file mapping Spark feature numbering to the real labels of the input features (e.g. "feature 0,cg13869341")
 * - a text file with the MLlib Random Forest debug string
 * - the output path
 * 
 * The output is composed of two text files:
 * - a list of features named after MLlib naming convention
 * - a list of features using the original feature labels (e.g. cpg dinuclotides)
 * 
 * A public method is also available as support to iterative algorithms.
 * 
 * @author fabrizio
 *
 */
public class TranslateRFModel {

	public static void main(String[] args) throws Exception{
		//Set<String> genesInvolved = extractGenesFromForest("D:/Experiments/DNAMeth/features2cpg.csv", "D:/Experiments/DNAMeth/tmp/EXP_009_RF.txt", "D:/Experiments/DNAMeth/output");	
		if(args.length<3)
			throw new Exception("Expected: mappingFile, randomForestDebugString, outpurDir");
		extractCpgFromForest(args[0], args[1], args[2], true);
	}

	private static void extractCpgFromForest(String mappingFile, String rfFile, String outputPath, boolean deleteIfExist) throws IOException{
		//prepare writing output
		FileManager ioManager = new FileManager();
		if(deleteIfExist)
			DirectoryManager.checkAndDelete(Paths.get(outputPath));
		File theDir = new File(outputPath);
		if(!theDir.exists()){
			theDir.mkdir();
		}

		//resulting genes
		Set<String> cpgInvolved = new TreeSet<String>();
		Set<String> featuresInvolved = new TreeSet<String>();

		//read mapping file
		FileManager manager = new FileManager();
		Map<String, String> mappingFeatures = manager.parseCommaTxt(mappingFile);

		//read decision tree
		String randomForest = new String(Files.readAllBytes(Paths.get(rfFile)));
		String[] trees = randomForest.trim().split("Tree [^:]*:");

		for(String tree: trees){
			tree = tree.trim();
			Pattern pat = Pattern.compile("feature [^ ]* ");
			Matcher mat = pat.matcher(tree);
			while (mat.find()){
				featuresInvolved.add(mat.group().trim());
				cpgInvolved.add(mappingFeatures.get(mat.group().trim()));
			}
		}

		ioManager.writeLines(featuresInvolved, outputPath+"/features.txt");
		ioManager.writeLines(cpgInvolved, outputPath+"/cpg.txt");
	}
	
	/**
	 * Support method to extract the list of features from a random forest debug string stored in a file, named after MLlib naming convention
	 * Useful for Random Forest Iterative Algorithm: write the list of features (named after "feature N") in a file.
	 * @param rfFile
	 * @param outputPath
	 * @param deleteIfExist
	 * @throws IOException
	 */
	public static void extractCpgFromForest(String rfFile, String outputPath, boolean deleteIfExist) throws IOException{
		//prepare writing output
		FileManager ioManager = new FileManager();
		if(deleteIfExist)
			DirectoryManager.checkAndDelete(Paths.get(outputPath));
		File theDir = new File(outputPath);
		if(!theDir.exists()){
			theDir.mkdir();
		}

		//resulting cpg
		Set<String> featuresInvolved = new TreeSet<String>();

		//read decision tree
		String randomForest = new String(Files.readAllBytes(Paths.get(rfFile)));
		String[] trees = randomForest.trim().split("Tree [^:]*:");

		for(String tree: trees){
			tree = tree.trim();
			Pattern pat = Pattern.compile("feature [^ ]* ");
			Matcher mat = pat.matcher(tree);
			while (mat.find()){
				featuresInvolved.add(mat.group().trim());
			}
		}

		ioManager.writeLines(featuresInvolved, outputPath+"/features.txt");
	}

}
