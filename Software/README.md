# README #

This project supports a study to adopt big data solutions and machine learning techniques to the analysis of DNA-methylation dataset of various diseases, like Breast Cancer. This package can be applied to similar research, or to other datasets compatible with application requirements.
  
### What is the research about? ###
* We wanted to extend the iterative classification algorithm implemented by [Camur](http://dmb.iasi.cnr.it/camur.php), in order to be able to cope with large datasets. To achieve this goal, we used Apache Spark (in standalone mode and on top of a Hadoop cluster) and MLlib (Spark's machine learning library). We carried out experiments with both Decision Trees and Random Forests, but Random Forests showed better results in terms of F-measure and performance. Then, we adopted a modified version of the Camur iterative algorithm with features deletion; we were able to extract many models and many candidate genes; results can be provided to biologists to check if extracted genes can be drivers for the related cancer.  
* We also applied feature selection (Chi-Squared method) to the original dataset, to check if reducing dimensionalities could lead to better precision. 

### What is this repository for? ###

This repository contains code that can be executed on Apache Spark for the analysis of Genomics data (or similar datasets). There are many applications that allow to compute Decision Trees or Random Forests, as well as to apply feature selection.

### How do I get set up? ###

Once you produce the JAR file "camur-0.0.1-SNAPSHOT.jar", you can execute various JAVA applications using the "java" command or submitting the JAR to a Spark instance. In order to submit machine learning jobs, Spark must be already installed on your machine.

### What I can do with this package? ###

##### support
* LoadModel: Load a MLlib decision tree or a random forest model and write its debug string to a file. (Spark Required!)
* TranslateRFModel: Given the MLlib Random Forest debug string and a mapping file Spark_Features -> Experiment_Features, extracts the list of Experiment Features (i.e. CpG dinuclotides) from the random forest debug string. In fact, MLlib renames features as "feature N", where N is an integer from 0 to the number of features minus one. This application returns a file with the list of features named after MLlib naming convention, and using the original feature labels.
* TranslateListFeatures2genes: Produce a CSV file mapping: MLlib feature name -> original feature name (e.g. CpG dinuclotides) -> gene name

##### data_preparation
* ExtractNFeatures: Extract first N features from CSV (comma separated). It adds the first and the last column. The header is maintained.
* CSVExtractHeader: It allows two things: (1) Produce a CSV file mapping Spark MLlib feature labels to original feature labels, and (2) Extract in a TEXT file the first column of features (the second column of the input CSV file) (Spark is required)
* MappingGenesCPG: Produce a CSV (comma separated) mapping CpGs to Genes.

##### experiments
* 

### Examples of Comands with arguments ###

* Translate Spark model to tree string  
*java -Xmx4096m -cp "/home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar:/usr/local/spark-2.1.0-bin-hadoop2.7/jars/\*" it.cnr.camur.support.TranslateRFModel /home/fabrizio/Experiments/DNAMeth/features2cpg.csv /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/10/forest.txt /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/10/features*

* Merge list of extracted features  
*cat /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/9/features/mergedFeatures.txt /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/10/features/features.txt > /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/10/features/mergedFeatures.txt*

* Submit Random Forest job  
*spark-submit --class "it.cnr.camur.experiments.Classifier_RandomForest" --conf "spark.driver.memory=12g" --master local[7] /home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar 5 16 5 /home/fabrizio/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/11 /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/10/features/mergedFeatures.txt 2>&1 > ./log &*

* Submit Iterative Random Forest job  
*spark-submit --class "it.cnr.camur.experiments.Classifier_Iterat_RandomForest" --conf "spark.driver.memory=18g" --master local[7] /home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar 1000 0.98 5 16 5 /home/fabrizio/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/17 /home/fabrizio/Experiments/DNAMeth/features2cpg.csv /home/fabrizio/Experiments/DNAMeth/cpg2genes.csv 2>&1 > ./log &*

* Submit Decision Tree job  
*spark-submit --class "it.cnr.camur.experiments.Classifier_DecisionTree" --conf "spark.driver.memory=18g" --master local[7] /home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar 5 16 512 /home/fabrizio/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/11 2>&1 > ./log &*

* Chi Feature Selection: [0,1] features, as CpG islands  
*spark-submit --class "it.cnr.camur.experiments.FeatureSelection_ChiSquared_Zero_One" --conf "spark.driver.memory=12g" --master local[7] /home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar 5 16 false /home/fabrizio/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv /home/fabrizio/Experiments/DNAMeth/EXPERIMENTS/20 /home/fabrizio/Experiments/DNAMeth/features2cpg.csv /home/fabrizio/Experiments/DNAMeth/cpg2genes.csv 2>&1 > ./log &*

* Extract first N columns, plus first and last  
*java -Xmx4096m -cp "/home/fabrizio/Experiments/JAR/camur-0.0.1-SNAPSHOT.jar" it.cnr.camur.data_preparation.ExtractNFeatures 10 /home/fabrizio/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv /home/fabrizio/Experiments/DNAMeth/DNAMeth_first10.csv*