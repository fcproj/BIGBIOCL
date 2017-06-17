package com.github.fcproj.bigbiocl.data_preparation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Extract the Nth feature from a CSV (comma separated). It adds the first and the last column. The header is maintained.
 * @author fabrizio
 *
 */
public class ExtractNthFeature {
	
	/**
	 * Extract Nth features from CSV (comma separated). It adds the first and the last column. The header is maintained.
	 * @param args: n index of feature to extract (features start from second column with index 0), inputFile full path, outputFile full path
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		if(args.length<3)
			throw new Exception("Expected: n index of feature to extract (features start from second column with index 0), nputFile full path, outputFile full path");
		
		int n = Integer.parseInt(args[0]);
		String inputFile = args[1];
		String outputFile = args[2];

		BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
		FileManager writer = new FileManager();

		File input1 = new File(inputFile);
		Scanner scanner = new Scanner(new FileReader(input1));

		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				String[] split = line.split(",");
				
				//build the line
				String newline = split[0]+","+split[n+1]+","+split[split.length-1];

				writer.addLine(newline, out);
			}
		}
		finally {
			scanner.close();
			out.close();
		}

	}

}
