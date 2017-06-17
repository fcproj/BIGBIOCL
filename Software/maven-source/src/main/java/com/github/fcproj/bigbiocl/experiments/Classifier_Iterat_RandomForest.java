package com.github.fcproj.bigbiocl.experiments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;

import com.github.fcproj.bigbiocl.support.TranslateRFModel;
import com.github.fcproj.bigbiocl.util.DirectoryManager;
import com.github.fcproj.bigbiocl.util.ErrorComputer;
import com.github.fcproj.bigbiocl.util.FileManager;
import com.github.fcproj.bigbiocl.util.LabeledPointManager;
import com.github.fcproj.bigbiocl.util.PathManager;

import scala.Tuple2;

/**
 * Iterative RandomForest classifier, BIGBIOCL algorithm. It removes all features of previous iterations, storing sub-iterations in a specific folder named after the iteration number.
 * Full support to be executed on YARN cluster.
 * 
 * In each sub-iteration folder, the list of extracted features in the current iteration and F-measure are provided. 
 * In addition, the computed Spark model is provided for each iteration: it can be used to load the model in a Spark client application (e.g. RandomForestModel.load method).
 * 
 * The application stops when a min F-measure or max iterations is reached.
 * 
 * The main output is a CSV of features extracted from all iterations, with CpG dinucleotides, and genes. Global statistics are also provided.
 * 
 * Application requires:
 * - maxNumberOfIterations: stopping condition in case minFMeasure is not reached
 * - minFMeasure: main stopping condition
 * - maxDepth: random forest parameter
 * - maxBins: random forest parameter
 * - number of trees: random forest parameter
 * - dataPath: the input CSV file
 * - output directory path
 * - mappingFile features2cpg: CSV comma separated that maps features named after Spark naming convention to column labels in the original CSV. (this file can be created using CSVExtractHeader application).
 * - mappingFile cpg2genes: CSV comma separated that maps CpG to genes (this file can be created using MappingGenesCPG application, which requires a BED file).
 * 
 * In a Yarn cluster, if you do not specify the path prefix "file" or "hdfs", defaults depend on the parameter:
 * - dataPath: it is hdfs path. To use a local directory, specify file:// as path prefix (if the input file is big, it is better to load it from hdfs) 
 * - output directory path: local directory in the server submitting the spark-submit job. This cannot be changed.
 * - features2cpg: local directory in the server submitting the spark-submit job. Support for "hdfs" paths.
 * - cpg2genes: local directory in the server submitting the spark-submit job. Support for "hdfs" paths.
 * If you use the defaults, the input file must be loaded in HDFS (e.g. hdfs dfs -put /camur/brca.csv /user/me/input)
 * 
 * INPUT file format: CSV file comma separated 
 * - the header is skipped: the algorithm starts from the second row 
 * - the first column is skipped (in our data, it contains the code of the tissue)
 * - the last column is the category
 * - features are Double values in any range
 * 
 * ******************************************************************************************************************************
 * Example of usage
 * 
 * Local mode
 * spark-submit --class "com.github.fcproj.bigbiocl.experiments.Classifier_Iterat_RandomForest" --conf "spark.driver.memory=18g" 
 * 		--master local[7] 
 * 		./camur-0.0.1-SNAPSHOT.jar 
 * 		1000 0.98 5 16 5 
 * 		./DNAMeth_MERGED_NOCONTROL_brca.csv 
 * 		./EXPERIMENT 
 * 		./features2cpg.csv 
 * 		./cpg2genes.csv
 * 
 * YARN all defaults
 * spark-submit --class "com.github.fcproj.bigbiocl.experiments.Classifier_Iterat_RandomForest" --master yarn --deploy-mode cluster 
 * 		--driver-memory 10g --executor-memory 10g --executor-cores 7 --queue default 
 * 		/camur/camur-0.0.1-TEST.jar 
 * 		100 0.97 5 16 5 
 * 		/user/me/input/kirp.csv 
 * 		/camur/TEST_HADOOP 
 * 		/camur/KIRP_mapping.csv 
 * 		/camur/cpg2genes.csv
 * 
 * YARN local input file
 * spark-submit --class "com.github.fcproj.bigbiocl.experiments.Classifier_Iterat_RandomForest" --master yarn --deploy-mode cluster 
 * 		--driver-memory 10g --executor-memory 10g --executor-cores 7 --queue default 
 * 		/camur/camur-0.0.1-TEST.jar 
 * 		100 0.97 5 16 5 
 * 		file:///camur/kirp.csv
 * 		/camur/TEST_HADOOP 
 * 		/camur/KIRP_mapping.csv 
 * 		/camur/cpg2genes.csv
 * 
 * YARN all input in HDFS
 * spark-submit --class "com.github.fcproj.bigbiocl.experiments.Classifier_Iterat_RandomForest" --master yarn --deploy-mode cluster 
 * 		--driver-memory 10g --executor-memory 10g --executor-cores 7 --queue default 
 * 		/camur/camur-0.0.1-TEST.jar 
 * 		100 0.97 5 16 5 
 * 		hdfs:///user/me/input/kirp.csv 
 * 		/camur/TEST_HADOOP 
 * 		hdfs:///user/me/input/KIRP_mapping.csv 
 * 		hdfs:///user/me/input/cpg2genes.csv
 * 
 * @author fabrizio
 *
 */
public class Classifier_Iterat_RandomForest {

	@SuppressWarnings({ "serial", "resource" })
	public static void main( String[] args ) throws Exception
	{
		//********************************************** LOADING **********************************************
		//check input parameters
		if(args.length<9)
			throw new Exception("Application requires: maxNumberOfIterations, minFMeasure, maxDepth, maxBins, number of trees, dataPath, output directory path, mappingFile features2cpg, mappingFile cpg2genes");

		//stopping conditions
		Integer maxIterations = Integer.parseInt(args[0]);
		Double minFMeasure = Double.parseDouble(args[1]);
		
		//output dir must be local
		String outputDir = args[6];
		if(outputDir.startsWith("hdfs") || outputDir.startsWith("local"))
			throw new Exception("Outpur directory is local by default. You cannot specify the prefix for the path");

		//support files/directories
		String datapath = args[5];
		String features2cpgPath = PathManager.getInstance().checkPathWithDefault(args[7], "file");
		String cpg2genesPath = PathManager.getInstance().checkPathWithDefault(args[8], "file");

		//check output directory
		DirectoryManager.checkAndDelete(Paths.get(outputDir));
		if(!Files.exists(Paths.get(outputDir))){
			new File(outputDir).mkdir();
		}

		//prepare writing output
		FileManager ioManager = new FileManager();

		//no support for HDFS. Output is local
		BufferedWriter statistics = new BufferedWriter(new FileWriter(outputDir+"/statistics"));

		//check if there are features to ignore
		Set<Integer> featuresToIgnore = new HashSet<Integer>();

		//for final mapping
		Map<String, String> cpg2genes = ioManager.parseCommaTxt(cpg2genesPath);
		Map<String, String> features2cpg = ioManager.parseCommaTxt(features2cpgPath);

		//random forest settings
		Integer numClasses = 2;
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		Integer numTrees = Integer.parseInt(args[4]); 
		String featureSubsetStrategy = "auto";
		String impurity = "gini";
		Integer maxDepth = Integer.parseInt(args[2]);//only supported <=30
		Integer maxBins = Integer.parseInt(args[3]);
		Integer seed = 5121985;

		//stopping conditions
		int iter = 0;
		double fMeasureCurr = 1.0;

		//********************************************** SPARK **********************************************
		SparkSession spark;	
		spark = SparkSession.builder()
				.appName("Classifier_Iterat_RandomForest")
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// Load and parse the data file.	
		long startTime = System.currentTimeMillis();
		JavaRDD<String> rawInputRdd = jsc.textFile(datapath);

		//function to skip the first line of partition 0 (hopefully, it will be the header)
		Function2<Integer, Iterator<String>, Iterator<String>> removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
			@Override
			public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
				if(ind==0 && iterator.hasNext()){
					iterator.next();
					return iterator;
				} else
					return iterator;
			}
		};
		JavaRDD<String> dataNoHeader = rawInputRdd.mapPartitionsWithIndex(removeHeader, false);

		//iterate
		long date = new Date().getTime();
		while(fMeasureCurr>=minFMeasure && iter<maxIterations) {
			try {
				long iterStartTime = System.currentTimeMillis();
				//manage directory
				String outputDirIteration = outputDir+"/"+iter;
				DirectoryManager.checkAndDelete(Paths.get(outputDirIteration));
				if(!Files.exists(Paths.get(outputDirIteration))){
					new File(outputDirIteration).mkdir();
				}

				ioManager.addLine("=======================================================", statistics);
				ioManager.addLine("=== ITERATION "+iter+" ===", statistics);

				//extract labeles points
				JavaRDD<LabeledPoint> parsedData = LabeledPointManager.prepareLabeledPoints(dataNoHeader, featuresToIgnore, null);

				// Split the data into training and test sets (30% held out for testing)
				JavaRDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.7, 0.3});
				JavaRDD<LabeledPoint> trainingData = splits[0];
				JavaRDD<LabeledPoint> testData = splits[1];

				// Train a RandomForest model.
				final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
						categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
						seed);

				long endBuildTime = System.currentTimeMillis();
				ioManager.addLine("BUILDING model: " + (endBuildTime - iterStartTime)/1000 + " seconds", statistics);

				//write the trees
				String forestName = outputDirIteration+"/forest.txt";
				ioManager.writeString(model.toDebugString(), forestName);

				// Evaluate model on test instances and compute test error
				JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
				ErrorComputer errorComp = new ErrorComputer();
				Double testErr = errorComp.fMeasure(predictionAndLabel);
				long endEvalTime = System.currentTimeMillis();
				ioManager.addLine("F-Measure on test data: " + String.format("%.2f", testErr*100)+"%", statistics);
				ioManager.addLine("EVALUATION of the model (test data): " + (endEvalTime - endBuildTime)/1000 + " seconds", statistics);
				predictionAndLabel.unpersist();

				//update stopping conditions
				fMeasureCurr = testErr;

				//evaluate the model on training data
				predictionAndLabel = trainingData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
				testErr = errorComp.fMeasure(predictionAndLabel);
				endEvalTime = System.currentTimeMillis();
				ioManager.addLine("F-Measure on training data: " + String.format("%.2f", testErr*100)+"%", statistics);
				ioManager.addLine("EVALUATION of the model (training data): " + (endEvalTime - endBuildTime)/1000 + " seconds", statistics);

				// Save the model: method default is an HDFS path, but application default is file
				try{
					String targetModel = PathManager.getInstance().checkPathWithDefault(outputDirIteration+"/RF_"+date, "file");
					model.save(jsc.sc(), targetModel);			
				}
				catch(Exception e){
					e.printStackTrace();
				}

				long endTime = System.currentTimeMillis();
				ioManager.addLine("ITERATION TIME " + (endTime - iterStartTime)/1000 + " seconds", statistics);

				//extract features and add to the list to be removed
				TranslateRFModel.extractCpgFromForest(forestName, outputDirIteration, false);
				featuresToIgnore.addAll(ioManager.parseFeatureNumbers(outputDirIteration+"/features.txt"));

				//release memory
				predictionAndLabel.unpersist(true);
				trainingData.unpersist(true);
				testData.unpersist(true);
				parsedData.unpersist(true);
				System.out.println("Memory Released");
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally {
				iter++;
			}
		}

		long endTime = System.currentTimeMillis();
		ioManager.addLine("\nOVERALL TIME " + (endTime - startTime)/1000 + " seconds", statistics);
		statistics.close();

		//write all extracted features
		//no support for HDFS. Output is local
		BufferedWriter out = new BufferedWriter(new FileWriter(outputDir+"/allFeatures.csv"));
		for(Integer i: featuresToIgnore)
			ioManager.addLine("feature "+i+";"+features2cpg.get("feature "+i)+";"+cpg2genes.get(features2cpg.get("feature "+i)), out);
		out.close();

		jsc.stop();
		spark.stop();
	}


}
