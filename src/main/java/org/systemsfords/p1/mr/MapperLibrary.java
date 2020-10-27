package org.systemsfords.p1.mr;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.systemsfords.p1.mr.SocketClient;


import javafx.util.Pair;


@SuppressWarnings("restriction")
public class MapperLibrary {
	
	final static int NEW_LINE = 10;

	public static void main(String args[]) throws IOException, Exception {
		
		//Reads the mapperUDF class and the files contents to be passed
		String mapperUDF = args[0];
		String fileName = args[1];
		
		Class<?> mapperUDFClass = Class.forName(mapperUDF);

		int startOffset = Integer.parseInt(args[2]);
		int endOffset = Integer.parseInt(args[3]);
		int N = Integer.parseInt(args[4]);
		String application = args[5];

		Map<Integer, StringBuilder> contentsOfFiles = new HashMap<Integer, StringBuilder>();
		
		String contentsFile = readFile(fileName, startOffset, endOffset);
		String[] lines = contentsFile.split("\\n");
		
		Method mapMethod = mapperUDFClass.getDeclaredMethod("map", String.class, String.class);
		
		for (String line : lines) {
			//Calling the map method using reflection
			List<Pair<String,String>> result = (List<Pair<String, String>>) 
												mapMethod.invoke(mapperUDFClass.newInstance(), null, line);
			if (result.size() > 0) {
				for (Pair<String,String> entry : result) {
					int hashCode = (entry.getKey().hashCode() & 0x7fffffff);
					int intermediate = hashCode % N;
					StringBuilder res;
					if (contentsOfFiles.containsKey(intermediate)) {
						res = contentsOfFiles.get(intermediate);
						res.append(entry.getKey() + ", " + entry.getValue() + "\n");
					} else {
						res = new StringBuilder(entry.getKey() + ", " + entry.getValue() + "\n");
					}
					//Storing the contents of each file in a hashmap
					contentsOfFiles.put(intermediate, res);
				}
			}
			
		}		

		//Writing the results to the intermediate file
		System.out.println("Writing the results of the map function to the intermediate file");
		writeToIntermediateFile(contentsOfFiles, application);
		
	}
	
	private static String getIntermediateFileName(int num, String application) {
		return "intermediateFile-"+application+"-"+num+".txt";
	}

	private static void writeToIntermediateFile(Map<Integer, StringBuilder> fileContents, String application) throws IOException, Exception {
		//Interprocess communication
		SocketClient client=new SocketClient();
	    client.startConnection("127.0.0.1", 6666);
		
	    for (Map.Entry<Integer, StringBuilder> entry: fileContents.entrySet()) {
			String intermediateFilePath =  System.getProperty("user.dir") + "/public/"+getIntermediateFileName(entry.getKey(), application);
			
			try {
				PrintWriter writer = new PrintWriter(new FileWriter(intermediateFilePath, true));
				writer.println(entry.getValue());
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
//			System.out.println("The intermediate File: "+intermediateFilePath);
			
		    client.sendMessage(intermediateFilePath);
		}
		client.sendMessage("done");
	    client.stopConnection();

	}

	/**
	 * @param fileName
	 * @param startOffset
	 * @param endOffset
	 * @return
	 */
	public static String readFile(String fileName, int startOffset, int endOffset) {
		StringBuilder sb = new StringBuilder();
		try {
			boolean foundEndSpace = false;
			boolean foundStartSpace = false;
			if (startOffset == 0) {
				foundStartSpace = true;
			}
			RandomAccessFile reader = new RandomAccessFile(fileName, "r");
			
			//Code to read entire rows instead of reading incomplete rows (reading bytes)
            if (!foundStartSpace) {
            	reader.seek(startOffset-1);
    			
                byte[] buffer = new byte[2];
                int numRead = reader.read(buffer, 0, 2);
    			if (buffer[0] == NEW_LINE || buffer[1] == NEW_LINE) {
    				foundStartSpace = true;	
    			} 
            }
			while (!foundStartSpace) {
				byte[] firstBuffer = new byte[50];
				reader.seek(startOffset);
				int n = reader.read(firstBuffer, 0, 50);
				for (int i = 0; i < firstBuffer.length; i++) {
					if (firstBuffer[i] == NEW_LINE || firstBuffer[i] == 0) {
						foundStartSpace = true;
						startOffset += i;
						break;
					}
				}
				if (!foundStartSpace) {
					startOffset = startOffset + 50;
				}
			}
			
			reader.seek(startOffset);
			int noBytes = endOffset - startOffset;
            int buffLength = (int) (noBytes);
			byte[] mainBuffer = new byte[buffLength];
			reader.read(mainBuffer, 0, buffLength);
			sb = new StringBuilder(new String(mainBuffer));
			if (mainBuffer[buffLength - 1] == NEW_LINE || mainBuffer[buffLength - 1] == 0) {
				foundEndSpace = true;
			}
			buffLength = startOffset + buffLength;
			while (!foundEndSpace) {
				byte[] secondBuffer = new byte[50];
	            reader.seek(buffLength);
	            int n = reader.read(secondBuffer, 0, 50);

	            for (byte b: secondBuffer) {
	            	if (b != NEW_LINE && b != 0) {
	            		sb.append((char)b);
	            	} else {
	            		foundEndSpace = true;
	            		break;
	            	}
	            }
	            buffLength = buffLength + 50;
			}
                        
			reader.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
}