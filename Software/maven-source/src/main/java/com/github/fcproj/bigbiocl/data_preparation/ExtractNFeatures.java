package com.github.fcproj.bigbiocl.data_preparation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Extract first N features from CSV (comma separated). It adds the first and the last column. The header is maintained.
 * @author fabrizio
 *
 */
public class ExtractNFeatures {

	/**
	 * Extract first N features from CSV (comma separated). It adds the first and the last column. The header is maintained.
	 * @param args: n number of features, inputFile full path, outputFile full path
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		if(args.length<3)
			throw new Exception("Expected: number of features, nputFile full path, outputFile full path");
		
		int n = Integer.parseInt(args[0]);
		String inputFile = args[1];
		String outputFile = args[2];

		BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
		FileManager writer = new FileManager();

		//file1
		File input1 = new File(inputFile);
		Scanner scanner = new Scanner(new FileReader(input1));
		try {
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				String[] split = line.split(",");
				String newline = "";
				//first n features, plus the fist column
				for(int i=0; i<=n; i++)
					newline = newline+split[i]+",";
				//category
				newline = newline+split[split.length-1];
				writer.addLine(newline, out);
			}
		}
		finally {
			scanner.close();
			out.close();
		}

	}

}
