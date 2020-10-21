package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MasterLibrary {

	String intermediateFilePath;

	static String outputFilePath;

	public static class MapperCallable implements Callable<String> {
		String mapperUDF;
		String inputFilePath;
		int startOffset;
		int endOffset;
		int N;

		public MapperCallable(String mapperUDF, String inputFilePath, int startOffset, int endOffset, int N) {
			this.mapperUDF = mapperUDF;
			this.inputFilePath = inputFilePath;
			this.startOffset = startOffset;
			this.endOffset = endOffset;
			this.N = N;
		}

		@Override
		public String call() throws Exception {

			return callMapperLibrary(mapperUDF, inputFilePath, startOffset, endOffset, N);
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

	public static class ServerCallable implements Callable<Set<String>> {
		
		private ServerSocket serverSocket;
		static ConcurrentMap<String, String> listOfIntermediateFiles = new ConcurrentHashMap();
		private static int N;

		public ServerCallable(int N) {
			this.N = N;
		}

		@Override
		public Set<String> call() throws Exception {
			start(6666);
			stop();
			return listOfIntermediateFiles.keySet();
		}

		public void start(int port) throws Exception {
			System.out.println("Listening to Clients...");
			serverSocket = new ServerSocket(port);
			
			for(int i=0;i<N;i++)
				new ClientsHandler(serverSocket.accept()).start();
		}

		public void stop() throws Exception {
			serverSocket.close();
		}

		private static class ClientsHandler extends Thread {
			private Socket clientSocket;
			private ObjectInputStream ois;
			
			public ClientsHandler(Socket socket) {
				this.clientSocket = socket;
			}
			
			public void run() {
				
				try {
					ois = new ObjectInputStream(clientSocket.getInputStream());
					String intermediateFile = (String) ois.readObject();
					
					//waits for each of the mapper to complete
					while (!(intermediateFile.equals("done"))){
						listOfIntermediateFiles.put(intermediateFile, "random");
						intermediateFile = (String) ois.readObject();
					}
					
					clientSocket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

		// Reading the inputs from the config file
		String configFile = System.getProperty("user.dir") + "/public/configFile.txt";
		Map<String, String> configMap = readConfigFile(configFile);
		String outputFile = System.getProperty("user.dir") + configMap.get("outputFilePath");
		String mapperUDF = configMap.get("mapperUDF");
		String reducerUDF = configMap.get("reducerUDF");
		String inputFilePath = System.getProperty("user.dir") + configMap.get("inputFile");
		int noOfProcesses = Integer.parseInt(configMap.get("N"));
		
		//Deleting all the intermediate and output files present in the folder
		File folder = new File(System.getProperty("user.dir") + "/public/"); 
		final File[] files = folder.listFiles(new FilenameFilter() {
		    	public boolean accept(final File dir, final String name) {
		    		return name.matches("intermediateFile.*\\.txt") || name.matches("outputFile.*\\.txt");
		    	}
		});
		for (final File file : files ) {
		    if (!file.delete() ) {
		        System.err.println( "Can't remove " + file.getAbsolutePath() );
		    }
		}
		
		// Creating socket communication to listen to the mapper outputs
		Future<Set<String>> potentialListOfIntermediateFiles = createServerProcess(noOfProcesses);
		
		callMapperLibraryMultipleThreads(mapperUDF, inputFilePath, noOfProcesses);
		Set<String> listOfIntermediateFiles=potentialListOfIntermediateFiles.get();
		
		System.out.println("Final list of intermediate files:"+listOfIntermediateFiles);

		callReducerLibraryMultipleThreads(reducerUDF, listOfIntermediateFiles, outputFile, noOfProcesses);
		System.exit(0);
	}

	private static Future<Set<String>> createServerProcess(int noOfProcesses) throws InterruptedException, ExecutionException {
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		
		Future<Set<String>> future = executorService.submit((new ServerCallable(noOfProcesses)));
		
		return future;

		
	}

	private static void callReducerLibraryMultipleThreads(String reducerUDF, Set<String> listOfIntermediateFiles,
			String outputFile, int noOfProcesses) {

		// To run the processes parallely
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		
		int i = 0;
		for (String intermediateFile: listOfIntermediateFiles) {
			Future<String> future = executorService
					.submit(new ReducerCallable(reducerUDF, intermediateFile, outputFile+"outputFile"+i+".txt"));
			listOfFutures.add(future);
			i++;
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

	private static void callMapperLibraryMultipleThreads(String mapperUDF, String inputFilePath, int noOfProcesses) {

		File f = new File(inputFilePath);
		int fileSize = (int) f.length();
		int sizeSingleChunk = fileSize / noOfProcesses;

		// To run the processes parallely
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		for (int i = 0; i < noOfProcesses; i++) {
			int startOffset = i * sizeSingleChunk;
			int endOffset = i * sizeSingleChunk + sizeSingleChunk;
			Future<String> future = executorService
					.submit(new MapperCallable(mapperUDF, inputFilePath, startOffset, endOffset, noOfProcesses));
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
	}

	public static String callMapperLibrary(String mapperUDF, String inputFilePath, int startOffset, int endOffset,int N) {

		ProcessBuilder processBuilder = new ProcessBuilder().redirectOutput(ProcessBuilder.Redirect.INHERIT);
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.MapperLibrary", mapperUDF, inputFilePath, String.valueOf(startOffset),
				String.valueOf(endOffset), String.valueOf(N));
		processBuilder.redirectErrorStream(true);
		String intermediateFilePath = null;
		try {

			Process process = processBuilder.start();

			int exitCode = process.waitFor();
			System.out.println("\nMapper Exited with error code : " + exitCode);
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
			System.out.println("\nReducer Exited with error code : " + exitCode);
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
