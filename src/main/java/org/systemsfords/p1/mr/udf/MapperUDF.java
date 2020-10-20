package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;

import org.systemsfords.p1.mr.Mapper;

public class MapperUDF implements Mapper{
	public List<Pair<String, String>> map(String key, String value) {
		String processedContents = value.toLowerCase();
		processedContents = processedContents.replaceAll("\\p{Punct}", " ");
		String[] terms = processedContents.split("\\s+");
		List<Pair<String, String>> values = new ArrayList<Pair<String, String>>();

		for (String term : terms) {
			Pair<String, String> entry = new Pair(term, "1");
			values.add(entry);
//			fileContents.append(term + ", " + 1 + "\n");
		}
		//System.out.println(fileContents.toString());
		return values;
	}
	
}