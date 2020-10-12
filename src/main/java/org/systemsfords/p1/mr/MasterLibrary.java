package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MasterLibrary {

	String intermediateFilePath;

	String outputFilePath;

	public static void main(String[] args) throws IOException {

		String configFile = System.getProperty("user.dir") + "/public/configFile.txt";
		Map<String, String> configMap = readConfigFile(configFile);
		String outputFile = System.getProperty("user.dir") + configMap.get("outputFilePath") + "outputFile.txt";
		String mapperUDF = configMap.get("mapperUDF");
		String reducerUDF = configMap.get("reducerUDF");
		String inputFilePath = System.getProperty("user.dir") + configMap.get("inputFile");
		int noOfProcesses = Integer.parseInt(configMap.get("N"));
		String intermediateFilePath = callMapperLibrary(mapperUDF, inputFilePath);
		callReducerLibrary(reducerUDF, intermediateFilePath, outputFile);
	}

	public static String callMapperLibrary(String mapperUDF, String inputFilePath) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.MapperLibrary", mapperUDF, inputFilePath);
		processBuilder.redirectErrorStream(true);
		String intermediateFilePath = null;
		try {

			Process process = processBuilder.start();

			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;

			while ((line = reader.readLine()) != null) {
				// System.out.println("Output of process: "+line);
				intermediateFilePath = line;
			}

			int exitCode = process.waitFor();
			System.out.println("\nExited with error code : " + exitCode);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return intermediateFilePath;
	}

	public static void callReducerLibrary(String reducerUDF, String intermediateFilePath, String outputFilePath) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.ReducerLibrary", reducerUDF, intermediateFilePath, outputFilePath);
		processBuilder.redirectErrorStream(true);
		try {

			Process process = processBuilder.start();

			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}

			int exitCode = process.waitFor();
			System.out.println("\nExited with error code : " + exitCode);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, String> readConfigFile(String fileName) {
		Map<String, String> config = new HashMap<String, String>();
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr);
			String s;
			while ((s = br.readLine()) != null) {
				String[] content = s.split("=");
				config.put(content[0], content[1]);
			}
			fr.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return config;
	}
}
