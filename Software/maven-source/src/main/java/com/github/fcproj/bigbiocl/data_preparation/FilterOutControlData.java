package com.github.fcproj.bigbiocl.data_preparation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Given a CSV file, whose last column is a classification (tumoran, normal, control), returns a CSV without lines of control cases. Replaces NA with ?, if any.
 * @author fabrizio
 *
 */
public class FilterOutControlData {
	
	public static void main(String[] args) throws Exception{
		if(args.length<2)
			throw new Exception("Expected: input file path, outputfile");
		FilterOutControlData filter = new FilterOutControlData();
		filter.filter(args[1], args[0]);
	}
	
	private void filter(String fullOutputPath, String file1) throws IOException{

		BufferedWriter out = new BufferedWriter(new FileWriter(fullOutputPath));
		FileManager writer = new FileManager();

		//reading input file
		File input1 = new File(file1);
		Scanner scanner = new Scanner(new FileReader(input1));
		try {
			boolean first = true;
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(first){
					first = false;
					writer.addLine(line, out);
				}
				else {
					String[] split = line.split(",");
					String type = split[split.length-1];
					line = line.replaceAll("NA", "?");
					
					//add the line
					if(!(type.equalsIgnoreCase("control") || type.equalsIgnoreCase("undefined"))){
						writer.addLine(line, out);
					}
				}

			}
		}
		finally {
			scanner.close();
		}
	}

}
