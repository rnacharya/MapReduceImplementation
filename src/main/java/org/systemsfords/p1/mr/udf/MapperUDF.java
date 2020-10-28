package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;

import org.systemsfords.p1.mr.Mapper;

public class MapperUDF implements Mapper{
	
	/**
	 * Process each line by removing punctuations and split by space.
	 * For each word, output the word and 1 as its occurence
	 */
	public List<Pair<String, String>> map(String key, String value) {
		String processedContents = value.toLowerCase();
		processedContents = processedContents.replaceAll("\\p{Punct}", " ");
		String[] terms = processedContents.split("\\s+");
		List<Pair<String, String>> values = new ArrayList<Pair<String, String>>();

		for (String term : terms) {
			Pair<String, String> entry = new Pair(term, "1");
			values.add(entry);
		}
		return values;
	}
	
}