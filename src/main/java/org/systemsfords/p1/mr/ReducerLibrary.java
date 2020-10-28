
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
		
		//Read the particular reducer UDF to be called, the intermediate file to be processed and the output to be written to
		String reducerUDF = args[0];
		String intermediateFilePath = args[1];
		String outputFilePath = args[2];
		//System.out.println("ReducerUDF className: " + reducerUDF);
		
		//Get the class of the reducer UDF
		Class<?> reducerUDFClass = Class.forName(reducerUDF);

		//Read the contents stored in the intermediate file assigned to this reducer instance and process it
		String contentsFile = readFile(intermediateFilePath);
		
		//Store the contents required to be written to each of the output files
		Map<String, List<String>> reducerMap = new HashMap<String, List<String>>();
		
		//Split the contents by row and process each row
		String[] contents = contentsFile.split("\n");

		for (String content : contents) {
			String[] terms = content.split(", ");
			//Create a hashmap to group the contents by keys
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

		//Get the reduce method defined in reducer UDF
		Method reduceMethod = reducerUDFClass.getDeclaredMethod("reduce", String.class, List.class);

		StringBuilder finList = new StringBuilder();
		for (Map.Entry<String, List<String>> entry : reducerMap.entrySet()) {
			//Call the reducer UDF with the respective key and values
			List<String> result = (List<String>) reduceMethod.invoke(reducerUDFClass.newInstance(), entry.getKey(),
					entry.getValue());
			finList.append(entry.getKey() + " " + result.toString() + "\n");
		}

		//Write the processed values to the output files
		writeToFile(finList.toString(), outputFilePath);
	}

	/**
	 * Function to read the intermediate file
	 * @param fileName
	 * @return
	 */
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

	/**
	 * Writing the processed contents to the output file specified
	 * @param fileContents
	 * @param outputFilePath
	 * @throws IOException
	 * @throws Exception
	 */
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
	    //Notifying the master that the reducer process has finished its execution
	    client.sendMessage("done");
		System.out.println("Successfully wrote to output file: "+outputFilePath);
		client.stopConnection();
	}
}
