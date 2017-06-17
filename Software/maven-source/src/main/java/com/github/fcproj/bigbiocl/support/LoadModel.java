package com.github.fcproj.bigbiocl.support;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Load a MLlib decision tree or a random forest model and write its debug string to a file.
 * It requires Spark already installed on the machine: it uses Spark in local mode, 1 core.
 * Input parameters are:
 * - the directory containing the Spark model (decision tree or random forest)
 * - the mode: tree or forest
 * - the fullpath of the output file
 * The output file contains the debug string of the model
 * @author fabrizio
 *
 */
public class LoadModel {

	public static void main(String[] args) throws Exception{
		if(args.length<3)
			throw new Exception("Expected: Spark model directory, 'forest' or 'tree', output file path");
		String mode = args[1];
		if(mode.equalsIgnoreCase("tree"))
			loadDecisionTreeModel(args[0], args[2]);
		else
			loadRandomForestModel(args[0], args[2]);
	}

	private static void loadDecisionTreeModel(String modelPath, String outputfile) throws IOException{

		SparkSession spark = getSparkSession();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		FileManager ioManager = new FileManager();

		DecisionTreeModel sameModel = DecisionTreeModel.load(jsc.sc(), modelPath);
		//System.out.println(sameModel.toDebugString());
		ioManager.writeString(sameModel.toDebugString(), outputfile);

		jsc.close();
	}
	
	private static void loadRandomForestModel(String modelPath, String outputfile) throws IOException{

		SparkSession spark = getSparkSession();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		FileManager ioManager = new FileManager();

		RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),modelPath);
		//System.out.println(sameModel.toDebugString());
		ioManager.writeString(sameModel.toDebugString(), outputfile);

		jsc.close();
	}
	
	private static SparkSession getSparkSession(){
		return SparkSession.builder()
				.master("local")
				.config("spark.driver.cores", 1)
				.appName("DNAMethJavaDecisionTree")
				.getOrCreate();
	}

}
