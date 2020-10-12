package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Mapper {

	public static void main(String args[])
			throws ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			InstantiationException, NoSuchMethodException, RuntimeException {
		String mapperUDF = args[0];
		Class<?> mapperUDFClass = Class.forName(mapperUDF);

		// System.out.println(Arrays.toString(args));
		String fileName = "public/sample.txt";
		String contentsFile = readFile(fileName);
		String[] mainArgs = new String[1];
		mainArgs[0]=contentsFile;
	    System.out.format("invoking %s.main()%n", c.getName());
	    main.invoke(null, (Object)mainArgs);
		Method methodcall1 = mapperUDFClass.getDeclaredMethod("map", (Object)mainArgs);

// invokes the method at runtime 
		methodcall1.invoke(mapperUDFClass.newInstance(), 19);
//		
//		String fileContents=map("public/sample.txt", contentsFile);
//		writeToIntermediateFile(fileContents);
	}

	private static void writeToIntermediateFile(String fileContents) {

		try {
			PrintWriter writer = new PrintWriter(new FileWriter("intermediateFile.txt", false));
			writer.println(fileContents);
			writer.close();
			System.out.println("intermediateFile.txt");
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