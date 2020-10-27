
package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.systemsfords.p1.mr.SocketClient;


public class ReducerLibrary {

	public static void main(String args[]) throws IOException, Exception {
		String reducerUDF = args[0];
		String intermediateFilePath = args[1];
		String outputFilePath = args[2];
		//System.out.println("ReducerUDF className: " + reducerUDF);
		Class<?> reducerUDFClass = Class.forName(reducerUDF);

		String contentsFile = readFile(intermediateFilePath);
		Map<String, List<String>> reducerMap = new HashMap<String, List<String>>();
		String[] contents = contentsFile.split("\n");

		for (String content : contents) {
			String[] terms = content.split(", ");
			if (terms.length == 2) {
				if (reducerMap.containsKey(terms[0])) {
					List<String> count = reducerMap.get(terms[0]);
					count.add(terms[1]);
				} else {
					List<String> count = new ArrayList<String>();
					count.add(terms[1]);
					reducerMap.put(terms[0], count);
				}
			}
			
		}

		Method reduceMethod = reducerUDFClass.getDeclaredMethod("reduce", String.class, List.class);

		StringBuilder finList = new StringBuilder();
		for (Map.Entry<String, List<String>> entry : reducerMap.entrySet()) {
			List<String> result = (List<String>) reduceMethod.invoke(reducerUDFClass.newInstance(), entry.getKey(),
					entry.getValue());
			finList.append(entry.getKey() + " " + result.toString() + "\n");
		}

		writeToFile(finList.toString(), outputFilePath);
	}

	public static String readFile(String fileName) {
		System.out.println("Reading intermediate file: " + fileName);
		StringBuilder sb = new StringBuilder();
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr);
			String s;
			while ((s = br.readLine()) != null) {
				sb.append(s + '\n');
			}
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}

	private static void writeToFile(String fileContents, String outputFilePath) throws IOException, Exception {
		SocketClient client=new SocketClient();
	    client.startConnection("127.0.0.1", 6666);
		
	    try {
			PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath, false));
			writer.println(fileContents);
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	    client.sendMessage("done");
		System.out.println("Successfully wrote to output file: "+outputFilePath);
		client.stopConnection();
	}
}
