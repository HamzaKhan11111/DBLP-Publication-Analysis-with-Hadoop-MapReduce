//make dir in hdfs
hadoop fs -mkdir /input2

//put data from current directory to input2 directory of hdfs
hadoop fs -copyFromLocal dblp.xml /input2

for Q1:	
	// compile myproject
	javac I20-0583_1/yearCount.java

	//create jar file
	jar -cvf myproject.jar -C I20-0583_1/ .

	//Run map-reduce
	hadoop jar myproject.jar yearCount /input2 /output
	
	//for output, run command:
	hdfs dfs -text /output/part-r-00000
	
	
//before running mapreduce for Q2, first remove output directory:
hdfs dfs -rm -r /output


for Q2:	
	// compile myproject
	javac I20-0583_2/yearCount.java

	//create jar file
	jar -cvf myproject.jar -C I20-0583_2/ .

	//Run map-reduce
	hadoop jar myproject.jar yearCount /input2 /output
	
	//for output, run command:
	hdfs dfs -text /output/part-r-00000



