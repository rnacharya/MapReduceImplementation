ant
java -cp target/mr-0.0.1-SNAPSHOT.jar org.systemsfords.p1.mr.MasterLibrary configFileInvertedIndex.txt
python3 testInvertedIndex.py
java -cp target/mr-0.0.1-SNAPSHOT.jar org.systemsfords.p1.mr.MasterLibrary configFile.txt
python3 testScript.py
java -cp target/mr-0.0.1-SNAPSHOT.jar org.systemsfords.p1.mr.MasterLibrary configFileEmployees.txt
python3 testEmployeeSalaries.py