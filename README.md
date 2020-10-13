# MapReduceImplementation
Contributers: Supreetha Somasundar(sbangaloreso@umass.edu), Smriti Murali(smritimurali@umass.edu) and Rachana Acharya(rnacharya@umass.edu)


Script to run this application:
runScript.sh present in the src/main/resources
This script will compile the code and run it. 

The configuration file used by this code to set some parameters will be found at public/configFile.txt. This file sets parameters such as input file name and path, output file path, N the number of mapper and reducer processes and the mapper and reducer UDF class names. 

The user defined functions are created in the src/main/java/org/systemsfords/p1/mr/udf directory. Any arbitrary class implementing the Mapper and the Reducer interfaces could be defined here, and the name specified in the config.txt to run. 

The output can be seen in the public/output.txt file. 

