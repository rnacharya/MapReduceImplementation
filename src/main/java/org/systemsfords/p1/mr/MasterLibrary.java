package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MasterLibrary {
	String mapperUDF;
	String reducerUDF;
	String intermediateFilePath;
	String outputFilePath;
	


	public MasterLibrary(String mapperUDF, String reducerUDF, String outputFilePath) {
		this.mapperUDF = mapperUDF;
		this.reducerUDF = reducerUDF;
		this.outputFilePath=outputFilePath;
	}

	public void masterEntry() {
		callMapperLibrary();
		callReducerLibrary();
	}

	// TODO: read config file
	public void callMapperLibrary() {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.MapperLibrary", mapperUDF);
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

	// TODO: read config file
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
}
