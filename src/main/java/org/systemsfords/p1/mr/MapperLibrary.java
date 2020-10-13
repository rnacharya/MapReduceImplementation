package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MapperLibrary {

	public static void main(String args[]) throws ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException {
		
		//Reads the mapperUDF class and the files contents to be passed
		String mapperUDF = args[0];
		String fileName = args[1];
		Class<?> mapperUDFClass = Class.forName(mapperUDF);

		String contentsFile = readFile(fileName);
		//Calling the map method using reflection
		Method mapMethod = mapperUDFClass.getDeclaredMethod("map", String.class, String.class);
		
		System.out.println("Calling the user defined map function");
		String result = (String) mapMethod.invoke(mapperUDFClass.newInstance(), null, contentsFile);

		//Writing the results to the intermediate file
		System.out.println("Writing the results of the map function to the intermediate file");
		writeToIntermediateFile(result);
	}

	private static void writeToIntermediateFile(String fileContents) {
		String intermediateFilePath =  System.getProperty("user.dir") + "/public/intermediateFile.txt";
		
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(intermediateFilePath, false));
			writer.println(fileContents);
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println(intermediateFilePath);

	}

	public static String readFile(String fileName) {
		StringBuilder sb = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
				    new FileInputStream(fileName), "UTF-8"));
			String s;
			while ((s = br.readLine()) != null) {
				sb.append(s);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
}