package org.systemsfords.p1.mr;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javafx.util.Pair;

public class MapperLibrary {
	
	final static int NEW_LINE = 10;

	public static void main(String args[]) throws ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException {
		
		//Reads the mapperUDF class and the files contents to be passed
		String mapperUDF = args[0];
		String fileName = args[1];
		Class<?> mapperUDFClass = Class.forName(mapperUDF);

		int startOffset = Integer.parseInt(args[2]);
		int endOffset = Integer.parseInt(args[3]);
		int N = Integer.parseInt(args[4]);

		Map<Integer, StringBuilder> contentsOfFiles = new HashMap<Integer, StringBuilder>();
		
		String contentsFile = readFile(fileName, startOffset, endOffset);
		String[] lines = contentsFile.split("\\n");
		System.out.println(Arrays.toString(lines));
		
		Method mapMethod = mapperUDFClass.getDeclaredMethod("map", String.class, String.class);
		
		for (String line : lines) {
			//Calling the map method using reflection
			List<Pair<String,String>> result = (List<Pair<String, String>>) 
												mapMethod.invoke(mapperUDFClass.newInstance(), null, line);
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
				
				contentsOfFiles.put(intermediate, res);
			}
		}		

		//Writing the results to the intermediate file
		System.out.println("Writing the results of the map function to the intermediate file");
		writeToIntermediateFile(contentsOfFiles);
		
	}
	
	private static String getIntermediateFileName(int num) {
		return "intermediateFile"+num+".txt";
	}

	private static void writeToIntermediateFile(Map<Integer, StringBuilder> fileContents) {
		for (Map.Entry<Integer, StringBuilder> entry: fileContents.entrySet()) {
			String intermediateFilePath =  System.getProperty("user.dir") + "/public/"+getIntermediateFileName(entry.getKey());
			
			try {
				PrintWriter writer = new PrintWriter(new FileWriter(intermediateFilePath, true));
				writer.println(entry.getValue());
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			System.out.println(intermediateFilePath);
		}
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
			int noBytes = endOffset - startOffset;
            int buffLength = (int) (noBytes);
			
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
			byte[] mainBuffer = new byte[buffLength];
			reader.read(mainBuffer, 0, buffLength);
			sb = new StringBuilder(new String(mainBuffer));

			if (mainBuffer[buffLength - 1] == NEW_LINE || mainBuffer[buffLength - 1] == 0) {
				foundEndSpace = true;
			}
			
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
                        
			System.out.println("-------------------");
			System.out.println(sb.toString());
			System.out.println("-------------------");
			reader.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
}