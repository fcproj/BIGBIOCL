package com.github.fcproj.bigbiocl.data_preparation;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.fcproj.bigbiocl.util.FileManager;

public class CSVExtractHeader {

	/**
	 * It allows two things:
	 * - Produce a CSV file mapping Spark MLlib feature labels to original feature labels.
	 * - Extract in a TEXT file the first column of features (the second column of the input CSV file)
	 * Spark is required.
	 * 
	 * Input:
	 * - Mode: 0 for the extraction of the mapping of Spark MLlib feature labels to original feature labels
	 * - outpur Full path
	 * - the start index of the first column to extract
	 * - number of columns to exclide from the end
	 * - csvSeparator
	 * 
	 * @param args mode, full path to the input CSV file, outpur Full path, column start index, number of columns to exclude from the end, csvSeparator
	 * @throws Exception 
	 * 
	 */
	public static void main(String[] args) throws Exception{

		if(args.length<6)
			throw new Exception("Expected: mode, full path to the input CSV file, outpur Full path, the start index of the "
					+ "first column to extract, "
					+ "number of columns to exclude from the end, csvSeparator");

		String mode = args[0];
		String appName = "CSVExtractHeader";
		String datapath = args[1];
		String outpurFullpath = args[2];
		String first = args[3];
		String removeFromLast = args[4];
		String csvSeparator = args[5];

		CSVExtractHeader extractor = new CSVExtractHeader();

		if(mode.equals("0"))
			extractor.extractFeaturesFromHeader(appName, datapath, outpurFullpath, Integer.parseInt(first), Integer.parseInt(removeFromLast), csvSeparator);
		else
			extractor.extractFirstLineInColumn(appName, datapath, outpurFullpath, Integer.parseInt(first), Integer.parseInt(removeFromLast), csvSeparator);
	}

	/**
	 * Given a CSV file, produces a CSV file mapping Spark MLlib feature labels to original feature labels.
	 * In order to skip columns at the beginning and at the end (e.g. because they are not features, but classes or other), two parameters are provided.
	 *
	 * @param appName name of the Spark app
	 * @param datapath full path to the input CSV file
	 * @param outpurFullpath full path to the output CSV file
	 * @param first the start index of the first column to extract
	 * @param removeFromLast number of columns to exclide from the end
	 * @param csvSeparator the character used as CSV separator
	 * @throws IOException
	 */
	public void extractFeaturesFromHeader(String appName, String datapath, String outpurFullpath, int first, int removeFromLast, String csvSeparator) throws IOException{

		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//extract the header
		JavaRDD<String> data = jsc.textFile(datapath);
		final String header = data.first();
		FileManager writer = new FileManager();
		writer.extractFeaturesFromCSVHeader(header, first, removeFromLast, outpurFullpath, csvSeparator);

		jsc.close();
	}

	/**
	 * Extracts in a TEXT file the first column of features (the second column of the input CSV file)
	 * @param appName name of the Spark app
	 * @param datapath full path to the input CSV file
	 * @param outpurFullpath full path to the output CSV file
	 * @param first the start index of the first column to extract
	 * @param removeFromLast number of columns to exclide from the end
	 * @param csvSeparator
	 * @throws IOException
	 */
	public void extractFirstLineInColumn(String appName, String datapath, String outpurFullpath, int first, int removeFromLast, String csvSeparator) throws IOException{

		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//extract the header
		JavaRDD<String> data = jsc.textFile(datapath);
		//final String header = data.first();
		final String header =  data.take(2).get(1);
		FileManager writer = new FileManager();
		writer.extractElementsToNewLine(header, first, removeFromLast, outpurFullpath, csvSeparator);

		jsc.close();
	}
}
