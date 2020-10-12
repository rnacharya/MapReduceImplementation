package org.systemsfords.p1.mr.udf;

import java.io.FileWriter;
import java.io.PrintWriter;

public class MapperUDF {
	public static String map(String key, String value) {
		String processedContents = value.replaceAll("\\p{Punct}", " ");
		String[] terms = processedContents.split("\\s+");
		StringBuilder fileContents = new StringBuilder();

		for (String term : terms) {
			fileContents.append(term + ", " + 1 + "\n");

		}
		System.out.println("Mapper UDF output: "+fileContents.toString());
		return fileContents.toString();
	}
	
}