all: lc.jar
clean:
	rm -rf *.class *.jar

lc.jar: Gaps.class
	jar cf lc.jar *.class

Gaps.class: Gaps.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar *.java
run: lc.jar
	hadoop fs -rm -r -f output/
	hadoop jar lc.jar Gaps input/vehicle/ output/
	hadoop fs -cat output/part-r-00000 > output.txt
sample: lc.jar
	hadoop fs -rm -r -f output/
	hadoop jar lc.jar Gaps input/vehicle_sample/ output/
	hadoop fs -cat output/part-r-00000 > output.txt
