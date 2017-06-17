package com.github.fcproj.bigbiocl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class FileManager {

	/**
	 * Given a CSV header, map CSV fields to Spark feature numbering.
	 * Spark numbers start from 0. 
	 * "first" defines the index of the first feature in the header
	 * "removeFromLast" defines how many fields should be excluded from right side
	 * @param header
	 * @param first the start index of the first column to extract
	 * @param removeFromLast number of columns to exclide from the end
	 * @param separator separator of the CSV, like , or ;
	 * @param outputFullPath
	 * @throws IOException
	 */
	public void extractFeaturesFromCSVHeader(String header, int first, int removeFromLast, String outputFullPath, String separator) throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFullPath));
		//prepare the output
		String[] split = header.split(separator);
		int j = 0;
		for(int i=first; i<(split.length-removeFromLast); i++){
			this.addLine("feature "+j+","+split[i], out);
			j++;
		}
		out.close();
	}

	public void extractElementsToNewLine(String header, int first, int removeFromLast, String outputFullPath, String separator) throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFullPath));
		//prepare the output
		String[] split = header.split(separator);
		for(int i=first; i<(split.length-removeFromLast); i++){
			this.addLine(split[i], out);
		}
		out.close();
	}

	/**
	 * Write a set of lines
	 * Add a timestamp at the beginning #yyyy/MM/dd HH:mm:ss
	 * @param lines a set of string
	 * @param filePath the fullpath of the file, together with file name and extension
	 * @throws IOException
	 */
	public void writeLines(Collection<String> lines, String filePath) throws IOException{
		//file writer
		BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
		for(String line: lines){
			out.write(line);
			out.newLine();
		}
		out.flush();
		out.close();
	}

	/**
	 * Add a line on a text file already existing. 
	 * @param line the line to add
	 * @param out the buffer
	 * @throws IOException
	 */
	public void addLine(String line, BufferedWriter out) throws IOException{
		out.write(line);
		out.newLine();
		out.flush();
	}

	/**
	 * Write a String in a text file
	 * @param content the string to be written
	 * @param filePath the fullpath of the file, together with file name and extension
	 * @throws IOException
	 */
	public void writeString(String content, String filePath) throws IOException{
		//file writer
		BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
		out.write(content);
		out.flush();
		out.close();
		//System.out.println("written: "+filePath);
	}

	/**
	 * Write a Map of String on a text file. Each line is a map entry in the form key=value
	 * Add a timestamp at the beginning #yyyy/MM/dd HH:mm:ss
	 * @param string2string a map of string
	 * @param filePath the fullpath of the file, together with file name and extension
	 * @throws IOException
	 */
	public void writeMapString(Map<String, String> string2string, String filePath) throws IOException{
		//file writer
		BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
		for(String title: string2string.keySet()){
			out.write(title);
			out.write(",");
			out.write(string2string.get(title));
			out.newLine();
		}
		out.flush();
		out.close();
	}

	/**
	 * Parse a text file of lines of features. No support for HDFS.
	 * Returns the list of feature numbers, removing the word "feature" from each line.
	 * 
	 * @param filepath the physical path of the file, without prefix (no hdfs or file)
	 * @return the list of feature numbers
	 * @throws IOException 
	 */
	public Set<Integer> parseFeatureNumbers(String filepath) throws IOException{
		Set<Integer> result = new HashSet<Integer>();
		File input = new File(filepath);
		Scanner scanner = new Scanner(new FileReader(input));
		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(line!=null && !line.startsWith("#") && line.length()>0){
					try{
						if(line.contains(";"))
							line = line.split(";")[0];
						result.add(Integer.parseInt(line.toLowerCase().replace("feature", "").trim()));
					}
					catch(Exception e){
					}
				}
			}
		}    
		finally {
			//ensure the underlying stream is always closed
			//this only has any effect if the item passed to the Scanner
			//constructor implements Closeable (which it does in this case).
			scanner.close();
		}
		return result;
	}

	/**
	 * Parse a text file of lines. Support for HDFS and FILE prefixes
	 * @param filepath the physical path of the file
	 * @return the set of lines
	 * @throws IOException 
	 */
	public Set<String> parseLines(String filepath) throws IOException{
		Set<String> result = new HashSet<String>();
		//Support for hdfs paths
		Configuration conf = new Configuration();
		Path path = new Path(filepath); 
		FileSystem fs = path.getFileSystem(conf);
		Scanner scanner = new Scanner(new InputStreamReader(fs.open(path)));
		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(line!=null && !line.startsWith("#") && line.length()>0){
					try{
						result.add(line);
					}
					catch(Exception e){
					}
				}
			}
		}    
		finally {
			//ensure the underlying stream is always closed
			//this only has any effect if the item passed to the Scanner
			//constructor implements Closeable (which it does in this case).
			scanner.close();
		}
		return result;
	}

	/**
	 * Parse a text file of lines having a pattern key,value. Keys are unique, i.e. there aren't two lines with the same key.
	 * Support for HDFS and FILE prefixes
	 * Returns the mapping.
	 * 
	 * @param filepath the physical path of the file
	 * @return the mapping of a text file of lines having a pattern key,value with unique keys
	 * @throws IOException 
	 */
	public Map<String, String> parseCommaTxt(String filepath) throws IOException{
		Map<String, String> result = new HashMap<String, String>();
		//Support for hdfs paths
		Configuration conf = new Configuration();
		Path path = new Path(filepath); 
		FileSystem fs = path.getFileSystem(conf);
		Scanner scanner = new Scanner(new InputStreamReader(fs.open(path)));
		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(line!=null && !line.startsWith("#") && line.length()>0){
					String[] mapping = line.split(",");
					result.put(mapping[0], mapping[1]);
				}
			}
		}
		finally {
			//ensure the underlying stream is always closed
			//this only has any effect if the item passed to the Scanner
			//constructor implements Closeable (which it does in this case).
			scanner.close();
		}
		return result;
	}

	/**
	 * Parse a text file of lines having a pattern key,value. Keys are unique, i.e. there aren't two lines with the same key.
	 * Support for HDFS and FILE prefixes
	 * Returns the mapping.
	 * 
	 * @param filepath the physical path of the file
	 * @return the mapping of a text file of lines having a pattern key,value with unique keys
	 * @throws IOException 
	 */
	public Map<String, String> parseGenes2CpGBED(String filepath) throws IOException{
		Map<String, String> result = new TreeMap<String, String>();
		//Support for hdfs paths
		Configuration conf = new Configuration();
		Path path = new Path(filepath); 
		FileSystem fs = path.getFileSystem(conf);
		Scanner scanner = new Scanner(new InputStreamReader(fs.open(path)));
		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(line!=null && !line.startsWith("#") && line.length()>0){
					String[] mapping = line.split("\t");
					result.put(mapping[4], mapping[6]);
				}
			}
		}
		finally {
			//ensure the underlying stream is always closed
			//this only has any effect if the item passed to the Scanner
			//constructor implements Closeable (which it does in this case).
			scanner.close();
		}
		return result;
	}

}
