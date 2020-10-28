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
import java.util.concurrent.TimeUnit;

public class MasterLibrary {

	String intermediateFilePath;

	static String outputFilePath;

	public static class MapperCallable implements Callable<String> {
		String mapperUDF;
		String inputFilePath;
		int startOffset;
		int endOffset;
		int N;
		String application;

		public MapperCallable(String mapperUDF, String inputFilePath, int startOffset, int endOffset, int N, String application) {
			this.mapperUDF = mapperUDF;
			this.inputFilePath = inputFilePath;
			this.startOffset = startOffset;
			this.endOffset = endOffset;
			this.N = N;
			this.application = application;
		}

		public String call() throws Exception {
			return callMapperLibrary(mapperUDF, inputFilePath, startOffset, endOffset, N, application);
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

		public String call() throws Exception {
			callReducerLibrary(reducerUDF, intermediateFilePath, outputFilePath);
			return null;
		}
	}

	public static class ServerCallable implements Callable<Set<String>> {
		
		private ServerSocket serverSocket;
		static ConcurrentMap<String, String> listOfIntermediateFiles = new ConcurrentHashMap();
		private static int N;
		private static String mode;
		private ConcurrentMap<Integer, Integer> completedProcessCounts= new ConcurrentHashMap<Integer, Integer>();

		public ServerCallable(int N, String mode) {
			this.N = N;
			this.mode = mode;
		}

		public Set<String> call() throws Exception {
			start(6666);
			stop();
			return listOfIntermediateFiles.keySet();
		}

		public void start(int port) throws Exception {
			System.out.println("Listening to " + mode);
			serverSocket = new ServerSocket(port);
			
			for(int i=0;i<N;i++) {
				//System.out.println("Server callable: "+completedProcessCounts.size());
				new ClientsHandler(serverSocket.accept(), completedProcessCounts).start();
				
			}
			Thread.sleep(1000);
			//System.out.println("Completed process counts as per main thread "+completedProcessCounts.size());
			if(completedProcessCounts.size()<N) {
				System.out.println("Server callable: Recreating a new connection");
				new ClientsHandler(serverSocket.accept(), completedProcessCounts).start();
			}
		}

		public void stop() throws Exception {
			serverSocket.close();
		}

		private static class ClientsHandler extends Thread {
			private Socket clientSocket;
			private ObjectInputStream ois;
			ConcurrentMap<Integer, Integer> completedProcessCounts;
			
			public ClientsHandler(Socket socket, ConcurrentMap<Integer, Integer> completedProcessCounts) {
				this.clientSocket = socket;
				this.completedProcessCounts=completedProcessCounts;
			}
			
			public void run() {
				
				try {
					ois = new ObjectInputStream(clientSocket.getInputStream());
					String intermediateFile = (String) ois.readObject();
					
					//waits for each of the mapper to complete
					while (!(intermediateFile.equals("done"))){
						if (mode == "mapper") {
							listOfIntermediateFiles.put(intermediateFile, "random");
						}
						intermediateFile = (String) ois.readObject();
					}
					completedProcessCounts.put(clientSocket.hashCode(), 1);
					clientSocket.close();
					//System.out.println("Server callable: Process complete "+clientSocket.hashCode());
					//System.out.println("Completed Process Counts: "+completedProcessCounts.size());
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Exception thrown in Client Handler");
					
				}
				return;
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
		final String application = configMap.get("application");
		
		//Deleting all the intermediate and output files present in the folder
		File folder = new File(System.getProperty("user.dir") + "/public/"); 
		final File[] files = folder.listFiles(new FilenameFilter() {
		    	public boolean accept(final File dir, final String name) {
		    		return name.matches("intermediateFile-"+application+".*\\.txt") || name.matches("outputFile-"+application+".*\\.txt");
		    	}
		});
		for (final File file : files ) {
		    if (!file.delete() ) {
		        System.err.println( "Can't remove " + file.getAbsolutePath() );
		    }
		}
		
		// Creating socket communication to listen to the mapper outputs
		Future<Set<String>> potentialListOfIntermediateFiles = createServerProcess(noOfProcesses, "mapper");
		
		callMapperLibraryMultipleThreads(mapperUDF, inputFilePath, noOfProcesses, application);
		Set<String> listOfIntermediateFiles=potentialListOfIntermediateFiles.get();
		
		System.out.println("Final list of intermediate files:"+listOfIntermediateFiles);
		System.out.println();
		
		createServerProcess(noOfProcesses, "reducer");
		callReducerLibraryMultipleThreads(reducerUDF, listOfIntermediateFiles, outputFile, noOfProcesses, application);
		System.exit(0);
	}

	private static Future<Set<String>> createServerProcess(int noOfProcesses, String mode) throws InterruptedException, ExecutionException {
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		
		Future<Set<String>> future = executorService.submit((new ServerCallable(noOfProcesses, mode)));
		
		return future;
	}

	private static void callReducerLibraryMultipleThreads(String reducerUDF, Set<String> listOfIntermediateFiles,
			String outputFile, int noOfProcesses, String application) {

		// To run the processes parallely
		ExecutorService executorService = Executors.newFixedThreadPool(noOfProcesses);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		
		int i = 0;
		for (String intermediateFile: listOfIntermediateFiles) {
			Future<String> future = executorService
					.submit(new ReducerCallable(reducerUDF, intermediateFile, outputFile+"outputFile-"+application+"-"+i+".txt"));
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


	private static void callMapperLibraryMultipleThreads(String mapperUDF, String inputFilePath, int noOfProcesses, String application) throws InterruptedException {

		File f = new File(inputFilePath);
		int fileSize = (int) f.length();
		int sizeSingleChunk = fileSize / noOfProcesses;

		// To run the processes parallely
		ExecutorService executorService = Executors.newFixedThreadPool(noOfProcesses);
		List<Future<String>> listOfFutures = new ArrayList<Future<String>>();
		List<Callable<String>> callables=new ArrayList<Callable<String>>();
		for (int i = 0; i < noOfProcesses; i++) {
			int startOffset = i * sizeSingleChunk;
			int endOffset = i * sizeSingleChunk + sizeSingleChunk;

			Callable<String> callable=new MapperCallable(mapperUDF, inputFilePath, startOffset, endOffset, noOfProcesses, application);
			callables.add(callable);
		}
		List<String> intermediateFilePaths = new ArrayList<String>();
		listOfFutures=executorService.invokeAll(callables);
		executorService.shutdown();
		executorService.awaitTermination(5, TimeUnit.MINUTES);
	}

	public static String callMapperLibrary(String mapperUDF, String inputFilePath, int startOffset, int endOffset,int N, String application) {

		ProcessBuilder processBuilder = new ProcessBuilder().redirectOutput(ProcessBuilder.Redirect.INHERIT);
		processBuilder.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
				"org.systemsfords.p1.mr.MapperLibrary", mapperUDF, inputFilePath, String.valueOf(startOffset),
				String.valueOf(endOffset), String.valueOf(N), application);
		processBuilder.redirectErrorStream(true);
		String intermediateFilePath = null;
		try {

			Process process = processBuilder.start();

			int exitCode = process.waitFor();
			System.out.println("\nMapper Exited with error code : " + exitCode);
			process.destroy();
			if(exitCode!=0) {
				System.out.println("\nRestarting failed mapper : " );
				ProcessBuilder processBuilder1 = new ProcessBuilder().redirectOutput(ProcessBuilder.Redirect.INHERIT);
				processBuilder1.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
						"org.systemsfords.p1.mr.MapperLibrary", mapperUDF, inputFilePath, String.valueOf(startOffset),
						String.valueOf(endOffset), String.valueOf(N));
				processBuilder1.redirectErrorStream(true);
				Process process1 = processBuilder1.start();
				System.out.println("\nWaiting for previously failed mapper process to complete " );
				int exitCode1 = process1.waitFor();
				
				System.out.println("\nPrevioulsy failed Mapper Exited with error code : " + exitCode1);
				process1.destroy();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return intermediateFilePath;
	}

	public static void callReducerLibrary(String reducerUDF, String intermediateFilePath, String outputFilePath) {
		
		ProcessBuilder processBuilder = new ProcessBuilder().redirectOutput(ProcessBuilder.Redirect.INHERIT);
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
			System.out.println("Reducer Exited with error code : " + exitCode);
			process.destroy();
			if(exitCode!=0) {
				System.out.println("\nRestarting failed reducer : " );
				ProcessBuilder processBuilder1 = new ProcessBuilder().redirectOutput(ProcessBuilder.Redirect.INHERIT);
				processBuilder1.command("java", "-cp", System.getProperty("user.dir") + "/target/mr-0.0.1-SNAPSHOT.jar",
						"org.systemsfords.p1.mr.ReducerLibrary", reducerUDF, intermediateFilePath, outputFilePath);
				processBuilder1.redirectErrorStream(true);
				Process process1 = processBuilder1.start();
				System.out.println("\nWaiting for previously failed reducer process to complete " );
				int exitCode1 = process1.waitFor();
				
				System.out.println("\nPrevioulsy failed reducer Exited with error code : " + exitCode1);
				process1.destroy();
			}
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
