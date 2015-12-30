all: lc.jar
clean:
	rm -rf *.class *.jar

lc.jar: LaneCount.class
	jar cf lc.jar *.class

LaneCount.class: LaneCount.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar *.java
run: lc.jar
	hadoop fs -rm -r -f output/
	hadoop jar lc.jar LaneCount input/vehicle output/
	hadoop fs -cat output/part-r-00000 > output.txt
