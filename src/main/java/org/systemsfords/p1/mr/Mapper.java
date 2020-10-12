package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Mapper {

	public static void main(String args[]) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String mapperUDF = args[0];
		Class<?> mapperUDFClass = Class.forName(mapperUDF);

		String fileName = "public/sample.txt";
		String contentsFile = readFile(fileName);
		
		Method mapMethod = mapperUDFClass.getDeclaredMethod("map", String.class, String.class);
	 
	    String result = (String) mapMethod.invoke(null, fileName, contentsFile);
		
		writeToIntermediateFile(result);
	}

	private static void writeToIntermediateFile(String fileContents) {

		try {
			PrintWriter writer = new PrintWriter(new FileWriter("intermediateFile.txt", false));
			writer.println(fileContents);
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static String readFile(String fileName) {
		StringBuilder sb = new StringBuilder();
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr);
			String s;
			while ((s = br.readLine()) != null) {
				sb.append(s);
			}
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
}