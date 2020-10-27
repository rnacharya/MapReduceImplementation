package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;

import javafx.util.Pair;

public class MapperInvertedIndexUDF {
	
	private static int sceneNum = -1;

	public List<Pair<String, String>> map(String key, String value) {
		String[] jsonContent = value.split(" : ");
		List<Pair<String, String>> values = new ArrayList<Pair<String, String>>();

		if (jsonContent.length == 2) {
			jsonContent[0] = jsonContent[0].trim();
			jsonContent[1] = jsonContent[1].replaceAll("\\p{Punct}", "").trim();
			if (jsonContent[0].equals("\"sceneNum\"")) {
				sceneNum = Integer.parseInt(jsonContent[1]);
			} else if (jsonContent[0].equals("\"text\"")) {
				if (sceneNum != -1) {
					String[] tokens = jsonContent[1].split("\\s+");
					for(String token : tokens) {
						Pair<String, String> entry = new Pair(token, String.valueOf(sceneNum));
						values.add(entry);
					}
					sceneNum = -1;
				}
				
			}
		}		
		return values;
	}
}