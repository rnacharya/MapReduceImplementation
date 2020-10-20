package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MasterLibrary {

	String intermediateFilePath;

	static String outputFilePath;

	public static class MapperCallable implements Callable<String> {
		String mapperUDF;
		String inputFilePath;

		public MapperCallable(String mapperUDF, String inputFilePath) {
			this.mapperUDF = mapperUDF;
			this.inputFilePath = inputFilePath;
		}

		@Override
		public String call() throws Exception {
			return callMapperLibrary(mapperUDF, inputFilePath);
		}
	}

	public static class ReducerCallable implements Callable<String> {
		String reducerUDF;
		String intermediateFilePath;
		String outputFilePath;

		public ReducerCallable(String reducerUDF, String intermediateFilePath, String outputFilePath) {
			this.reducerUDF = reducerUDF;
			this.intermediateFilePath = intermediateFilePath;
			this.outputFilePath = outputFilePath;
		}

		@Override
		public String call() throws Exception {
			callReducerLibrary(reducerUDF, intermediateFilePath, outputFilePath);
			return null;
		}
	}

	public static void main(String[] args) throws IOException {

		// Reading the inputs from the config file
		String configFile = System.getProperty("user.dir") + "/public/configFile.txt";
		Map<String, String> configMap = readConfigFile(configFile);
		String outputFile = System.getProperty("user.dir") + configMap.get("outputFilePath") + "outputFile.txt";
		String mapperUDF = configMap.get("mapperUDF");
		String reducerUDF = configMap.get("reducerUDF");
		String inputFilePath = System.getProperty("user.dir") + configMap.get("inputFile");
		int noOfProcesses = Integer.parseInt(configMap.get("N"));

		String intermediateFilePath = callMapperLibraryMultipleThreads(mapperUDF, inputFilePath, noOfProcesses);
		
		callReducerLibraryMultipleThreads(reducerUDF, intermediateFilePath, outputFile, noOfProcesses);
		
		
		//callReducerLibrary(reducerUDF, intermediateFilePath1, outputFile);
	}

	private static void callReducerLibraryMultipleThreads(String reducerUDF, String intermediateFilePath, String outputFile, int noOfProcesses) {
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		for (int i = 0; i < 1; i++) {
			Future<String> future = executorService.submit(new ReducerCallable(reducerUDF, intermediateFilePath, outputFile));
			listOfFutures.add(future);
		}
		
		try {
			for (Future<String> future : listOfFutures) {
				future.get();

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		executorService.shutdown();
	}

	private static String callMapperLibraryMultipleThreads(String mapperUDF, String inputFilePath, int noOfProcesses) {
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		for (int i = 0; i < 1; i++) {
			Future<String> future = executorService.submit(new MapperCallable(mapperUDF, inputFilePath));
			listOfFutures.add(future);
		}
		List<String> intermediateFilePaths = new ArrayList<String>();
		try {
			for (Future<String> future : listOfFutures) {
				intermediateFilePaths.add(future.get());

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		executorService.shutdown();
		String intermediateFilePath=intermediateFilePaths.get(0);
		return intermediateFilePath;
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
