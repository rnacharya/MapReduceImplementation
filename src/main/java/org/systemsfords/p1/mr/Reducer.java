
package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 

public class Reducer {
	
	public static void main(String args[]) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String reducerUDF = args[0];
		String intermediateFilePath = args[1];
		Class<?> reducerUDFClass = Class.forName(reducerUDF);

		String contentsFile = readFile(intermediateFilePath);
		Map<String, List<String>> reducerMap = new HashMap<String, List<String>>();
		String[] contents = contentsFile.split("\n");
		
		for (String content : contents) {
			String[] terms = content.split(", ");
		    
			if (reducerMap.containsKey(terms[0])) {
		    	List<String> count = reducerMap.get(terms[0]);
		    	count.add(terms[1]);
		    }
			else {
				List<String> count = new ArrayList<String>();
				count.add(terms[1]);
				reducerMap.put(terms[0], count);
		    }
		}

		Method reduceMethod = reducerUDFClass.getDeclaredMethod("reduce", String.class, List.class);
		
		StringBuilder finList = new StringBuilder();
		for (Map.Entry<String, List<String>> entry: reducerMap.entrySet()) {
			List<String> result = (List<String>) reduceMethod.invoke(null, entry.getKey(), entry.getValue());
			finList.append(entry.getKey() + ", " + result.toString() + "\n");
		}
		
		writeToFile(finList.toString());
	}
	
	
	public static String readFile(String fileName) {
        StringBuilder sb  = new StringBuilder();
        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            String s;
            while((s = br.readLine()) != null) {
                sb.append(s + '\n');
            }
            fr.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return sb.toString();
    }
	
	private static void writeToFile(String fileContents) {

		try {
			PrintWriter writer = new PrintWriter(new FileWriter("outputFile.txt", false));
			writer.println(fileContents);
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
