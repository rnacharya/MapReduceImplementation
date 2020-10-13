package org.systemsfords.p1.mr.udf;

import org.systemsfords.p1.mr.Mapper;

public class MapperUDF implements Mapper{
	public String map(String key, String value) {
		String[] terms = value.split("\\s+");
		StringBuilder fileContents = new StringBuilder();

		for (String term : terms) {
			fileContents.append(term + ", " + 1 + "\n");
		}
		//System.out.println(fileContents.toString());
		return fileContents.toString();
	}
	
}