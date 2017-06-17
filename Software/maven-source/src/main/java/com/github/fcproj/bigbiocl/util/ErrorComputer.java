package com.github.fcproj.bigbiocl.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ErrorComputer {

	// error: wrong classifications/total classifications
	// INPUT: predicted, test
	public double wrongRate(JavaPairRDD<Double, Double> predictionAndLabel){
		return 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() 
		{
			public Boolean call(Tuple2<Double, Double> pl) {
				return !pl._1().equals(pl._2());
			}
		}).count() / predictionAndLabel.count();
	}
	
	// F=2PR/(P+R)
	// P=Tp/(Tp+Fp)
	// R=Tp/(Tp+Fn)
	// INPUT: predicted, test
	public double fMeasure(JavaPairRDD<Double, Double> predictionAndLabel){
		double tp =  1.0 * predictionAndLabel.filter(w->w._1() == 1.0).filter(pl-> pl._1().equals(pl._2())).count();
		double fp =  1.0 * predictionAndLabel.filter(w->w._1() == 1.0).filter(pl-> !pl._1().equals(pl._2()) ).count();
		double fn =  1.0 * predictionAndLabel.filter(w->w._1() == 0.0).filter(pl-> !pl._1().equals(pl._2())).count();
		//precision and recall
		double p = tp/(tp+fp);
		double r = tp/(tp+fn);
		return 2*p*r/(p+r);
	}
}
