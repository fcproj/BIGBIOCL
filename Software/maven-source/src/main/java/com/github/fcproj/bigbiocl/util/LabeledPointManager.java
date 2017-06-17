package com.github.fcproj.bigbiocl.util;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeledPointManager {
	
	/**
	 * Parse the date without header to extract labeledPoints
	 * The last column is the category
	 * The first column is the ID of the observation, to be removed
	 * There can be null values, denoted by ?. We use sparse vectors then.
	 * 
	 * @param dataNoHeader
	 * @param featureToBeIgnored can be null
	 * @param featureToInclude can be null
	 * @return
	 */
	@SuppressWarnings({ "serial" })
	public static JavaRDD<LabeledPoint> prepareLabeledPoints(JavaRDD<String> dataNoHeader, Set<Integer> featureToBeIgnored, Set<Integer> featureToInclude) {
		return dataNoHeader.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String line) throws Exception {
				String[] parts = line.split(",");
				//create array of values, skipping the first column
				Map<Integer, Double> index2value = new TreeMap<Integer, Double>();
				for(int i=1;i<parts.length-1;i++){
				//for(int i=1;i<3;i++){
					if(!parts[i].equals("?")){
						//feature numbers start from 0: here the column 0 is the ID of the experiment, so we have to match i-1
						if((featureToBeIgnored==null || !featureToBeIgnored.contains(i-1)) && (featureToInclude==null || featureToInclude.contains(i-1)))
							index2value.put(i-1, Double.parseDouble(parts[i]));
					}
				}
				int size = parts.length-2;
				int[] indices = new int[index2value.size()];
				double[] values = new double[index2value.size()];
				int i = 0;
				for(int index: index2value.keySet()){
					indices[i] = index;
					values[i] = index2value.get(index);
					i++;
				}
				//parse the label
				String label = parts[parts.length-1];
				double dLabel = 0;
				//tumoral or normal
				if(label.equalsIgnoreCase("tumoral"))
					dLabel = 1;
				return new LabeledPoint(dLabel,	new SparseVector(size, indices, values));
			}
		});
	}

	


}
