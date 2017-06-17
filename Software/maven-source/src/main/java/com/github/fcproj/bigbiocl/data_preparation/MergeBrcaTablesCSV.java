package com.github.fcproj.bigbiocl.data_preparation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import com.github.fcproj.bigbiocl.util.FileManager;

/**
 * Specific app to merge:
 * brca_normal_dnameth_table.csv
 * brca_tumoral_dnameth_table.csv
 * 
 * Ci sono due piccole modifiche da apportare:
 * 1. fare il merge delle due tabelle
 * 2. aggiungere la classe come ultima colonna
 * 3. sostituire tutti i valori NA con ?
 */
public class MergeBrcaTablesCSV {

	public static void main(String[] args) throws Exception{
		//String output = "D:/Experiments/DNAMeth/DNAMeth_MERGED_NOCONTROL_brca.csv";
		if(args.length<3)
			throw new Exception("Expected: file1, file2, outputfile");
		MergeBrcaTablesCSV merger = new MergeBrcaTablesCSV();
		merger.merge(true, args[2], args[0], args[1]);
	}

	private void merge(boolean excludeControl, String fullOutputPath, String file1, String file2) throws IOException{
		//String file1 = "D:/Experiments/DNAMeth/brca_normal_dnameth_table.csv";
		//String file2= "D:/Experiments/DNAMeth/brca_tumoral_dnameth_table.csv";

		BufferedWriter out = new BufferedWriter(new FileWriter(fullOutputPath));
		FileManager writer = new FileManager();

		System.out.println("...file 1");
		//file1
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
					String type = tissueType(split[0]);
					line = line.replaceAll("NA", "?");
					//edit the line
					if(!(excludeControl && (type.equalsIgnoreCase("control") || type.equalsIgnoreCase("undefined")))){
						if(line.endsWith(","))
							line = line+type;
						else
							line = line+","+type;
						writer.addLine(line, out);
					}
				}

			}
		}
		finally {
			scanner.close();
		}

		System.out.println("...file 2");
		//file2: no adding the header again!
		File input2 = new File(file2);
		scanner = new Scanner(new FileReader(input2));
		try {
			boolean first = true;
			//first use a Scanner to get each line
			while (scanner.hasNextLine()){
				String line = scanner.nextLine();
				if(first){
					first = false;
				}
				else {
					String[] split = line.split(",");
					String type = tissueType(split[0]);
					line = line.replaceAll("NA", "?");
					//edit the line
					if(!(excludeControl && (type.equalsIgnoreCase("control") || type.equalsIgnoreCase("undefined")))){
						if(line.endsWith(","))
							line = line+type;
						else
							line = line+","+type;
						writer.addLine(line, out);
					}
				}

			}
		}
		finally {
			scanner.close();
			out.close();
		}
	}

	/**
	 * Extract the status from the tissue code
	 * @param aliquot
	 * @return
	 */
	public String tissueType(String aliquot) {
		try {
			String[] pSplit = aliquot.split("-");
			String tumor_normal_ctrl = pSplit[3];
			if (tumor_normal_ctrl.startsWith("0"))
				return "tumoral";
			else if (tumor_normal_ctrl.startsWith("1"))
				return "normal";
			else if (tumor_normal_ctrl.startsWith("2"))
				return "control";
			else
				return "undefined";
		} catch (Exception e) {
			return "undefined";
		}
	}

}
