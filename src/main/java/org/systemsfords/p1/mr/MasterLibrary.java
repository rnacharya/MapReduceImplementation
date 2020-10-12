package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MasterLibrary {
	String mapperUDF;
	String reducerUDF;
	String intermediateFilePath;
	String inputFilePath;
	String outputFilePath;
	int noOfProcesses;

	public MasterLibrary(String mapperUDF, String reducerUDF, String outputFilePath, String inputFilePath, int noOfProcesses) {
		this.mapperUDF = mapperUDF;
		this.reducerUDF = reducerUDF;
		this.inputFilePath = System.getProperty("user.dir") + inputFilePath;
		this.outputFilePath = System.getProperty("user.dir") + outputFilePath;
		this.noOfProcesses = noOfProcesses;
	}
	
	public static void main(String args[]) {
//		String configFile = System.getProperty("user.dir") + args[0];
		String configFile = System.getProperty("user.dir") + "/public/configFile.txt";
		Map<String, String> configMap = readConfigFile(configFile);
		String outputFile = configMap.get("outputFilePath") + "outputFile.txt";
		MasterLibrary masLib = new MasterLibrary(configMap.get("mapperUDF"), 
				configMap.get("reducerUDF"), outputFile, configMap.get("inputFile"), Integer.parseInt(configMap.get("N")));
		masLib.callMapperLibrary();
		masLib.callReducerLibrary();
	}

	public void callMapperLibrary() {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.MapperLibrary", mapperUDF, this.inputFilePath);
		processBuilder.redirectErrorStream(true);

		try {

			Process process = processBuilder.start();

			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println("Output of process: "+line);
				intermediateFilePath = line;
			}

			int exitCode = process.waitFor();
			System.out.println("\nExited with error code : " + exitCode);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void callReducerLibrary() {
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
	
	public static Map<String,String> readConfigFile(String fileName) {
		Map<String,String> config = new HashMap<String, String>();
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
