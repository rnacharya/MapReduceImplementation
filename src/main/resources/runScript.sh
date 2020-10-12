cd ../java/
mvn clean build
cd ../target
java -cp mr-0.0.1-SNAPSHOT.jar "org.systemsfords.p1.mr.App" "org.systemsfords.p1.mr.udf.MapperUDF" "org.systemsfords.p1.mr.udf.ReducerUDF" "outputFile.txt"
