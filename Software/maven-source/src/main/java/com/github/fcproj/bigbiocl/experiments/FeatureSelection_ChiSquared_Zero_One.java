package com.github.fcproj.bigbiocl.experiments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.SparkSession;

import com.github.fcproj.bigbiocl.util.DirectoryManager;
import com.github.fcproj.bigbiocl.util.ErrorComputer;
import com.github.fcproj.bigbiocl.util.FileManager;
import com.github.fcproj.bigbiocl.util.LabeledPointManager;

import scala.Tuple2;

/**
 * Feature Selection using ChiSqSelector
 * Application requires: number of output features, number of bins for discretization, boolean to run or not the decision tree on the filtered dataset, dataPath, output directory path, mappingFile features2cpg, mappingFile cpg2genes
 * 
 * INPUT: CSV file (the header is skipped). The first column is skipped, the last column is the category, features are comma separated
 * Features are Double values [0,1] range. This is important for discretization
 * 
 * If the boolean is true, it computes a decision tree with maxBins=16 and maxDepth=5 on filtered data (70-30 the proportion training-test set)
 * 
 * Feature selection tries to identify relevant features for use in model construction. 
 * It reduces the size of the feature space, which can improve both speed and statistical learning behavior.
 * 
 * ChiSqSelector implements Chi-Squared feature selection. It operates on labeled data with categorical features. 
 * ChiSqSelector uses the Chi-Squared test of independence to decide which features to choose. It supports three selection methods: numTopFeatures, percentile, fpr:
 * 
 *  -  numTopFeatures chooses a fixed number of top features according to a chi-squared test. This is akin to yielding the features with the most predictive power.
 *  -  percentile is similar to numTopFeatures but chooses a fraction of all features instead of a fixed number.
 *  -  fpr chooses all features whose p-value is below a threshold, thus controlling the false positive rate of selection.
 * 
 * By default, the selection method is numTopFeatures, with the default number of top features set to 50. The user can choose a selection method using setSelectorType.
 * The number of features to select can be tuned using a held-out validation set.
 * 
 * @author fabrizio
 *
 */
public class FeatureSelection_ChiSquared_Zero_One {

	@SuppressWarnings({ "serial", "resource" })
	public static void main(String[] args) throws Exception{

		//check input parameters
		if(args.length<7)
			throw new Exception("Application requires: number of output features, number of bins for discretization, boolean to run or not the decision tree on the filtered dataset, dataPath, output directory path, mappingFile features2cpg, mappingFile cpg2genes");

		int numberOfFeatures = Integer.parseInt(args[0]);
		int numberOfBinsDiscretization = Integer.parseInt(args[1]);
		boolean extractFiltDataset = Boolean.parseBoolean(args[2]);
		String datapath = args[3];
		String outputPath = args[4];

		//for final mapping
		FileManager ioManager = new FileManager();
		BufferedWriter out = null;
		BufferedWriter statistics = null;
		Map<String, String> features2cpg = ioManager.parseCommaTxt(args[5]);
		Map<String, String> cpg2genes = ioManager.parseCommaTxt(args[6]);

		//check output directory
		DirectoryManager.checkAndDelete(Paths.get(outputPath));
		if(!Files.exists(Paths.get(outputPath))){
			new File(outputPath).mkdir();
		}

		long startTime = System.currentTimeMillis();

		//Properties set directly on the SparkConf take highest precedence, then flags passed to spark-submit or spark-shell, then options in the spark-defaults.conf file.
		SparkSession spark;
		spark = SparkSession.builder()
				//.master("local[4]")
				.appName("FeatureSelection_ChiSquared")
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> rawInputRdd = jsc.textFile(datapath);

		//===============================================================================================================================
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
		//dataNoHeader.cache();

		//extract labeles points
		JavaRDD<LabeledPoint> points = LabeledPointManager.prepareLabeledPoints(dataNoHeader, null, null);

		//===============================================================================================================================
		out = new BufferedWriter(new FileWriter(outputPath+"/discretization.txt"));
		ioManager.addLine(String.valueOf(points.take(1).get(0).features().size()), out);
		ioManager.addLine(points.take(1).toString(), out);

		// Discretize data in equal bins since ChiSqSelector requires categorical features
		// ChiSqSelector treats each unique value as a category
		final double divider = 100./ numberOfBinsDiscretization;
		JavaRDD<LabeledPoint> discretizedData = points.map(
				new Function<LabeledPoint, LabeledPoint>() {
					@Override
					public LabeledPoint call(LabeledPoint lp) {
						final double[] discretizedFeatures = new double[lp.features().size()];
						for (int i = 0; i < lp.features().size(); ++i) {
							discretizedFeatures[i] = Math.floor(lp.features().apply(i) * 100 / divider);
						}
						return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
					}
				}
				);
		ioManager.addLine(String.valueOf(discretizedData.take(1).get(0).features().size()), out);
		ioManager.addLine(discretizedData.take(1).toString(), out);
		out.close();

		//===============================================================================================================================
		// Create ChiSqSelector 
		ChiSqSelector selector = new ChiSqSelector(numberOfFeatures);
		final ChiSqSelectorModel transformer = selector.fit(discretizedData.rdd());

		//WRITE EXTRACTED FEATURES
		out = new BufferedWriter(new FileWriter(outputPath+"/features.csv"));
		for(int l: transformer.selectedFeatures())
			ioManager.addLine("feature "+l+";"+features2cpg.get("feature "+l)+";"+cpg2genes.get(features2cpg.get("feature "+l)), out);
		out.close();	

		//===============================================================================================================================
		if(extractFiltDataset){
			// Filter the top features from each feature vector
			JavaRDD<LabeledPoint> filteredData = points.map(
					new Function<LabeledPoint, LabeledPoint>() {
						@Override
						public LabeledPoint call(LabeledPoint lp) {
							return new LabeledPoint(lp.label(), transformer.transform(lp.features()));
						}
					}
					);

			statistics = new BufferedWriter(new FileWriter(outputPath+"/statistics.txt"));

			//RUN DEFAULT DECISION TREE
			long startBuildTime = System.currentTimeMillis();
			JavaRDD<LabeledPoint>[] splits = filteredData.randomSplit(new double[]{0.7, 0.3});
			JavaRDD<LabeledPoint> trainingData = splits[0];
			JavaRDD<LabeledPoint> testData = splits[1];
			Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
			final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, 2, categoricalFeaturesInfo, "gini", 4, 10);

			// Evaluate model on test instances and compute test error
			JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
			ErrorComputer errorComp = new ErrorComputer();
			Double testErr = errorComp.fMeasure(predictionAndLabel);
			long endEvalTime = System.currentTimeMillis();
			ioManager.addLine("F-Measure on test data: " + String.format("%.2f", testErr*100)+"%", statistics);
			ioManager.addLine("BUILD AND EVALUATION of the model (test data): " + (endEvalTime - startBuildTime)/1000 + " seconds", statistics);
			ioManager.addLine(model.toDebugString(), statistics);
		}

		//FLUSH STATISTICS
		long endTime = System.currentTimeMillis();
		if(statistics!=null){
			ioManager.addLine("OVERALL TIME " + (endTime - startTime)/1000 + " seconds", statistics);
			statistics.close();
		}
		else
			ioManager.writeString("OVERALL TIME " + (endTime - startTime)/1000 + " seconds", outputPath+"/statistics.txt");	

		jsc.stop();
		spark.stop();
	}

}
