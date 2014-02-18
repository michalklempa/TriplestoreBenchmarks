#!/bin/sh

for RUN in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 18 19 20; do
echo $RUN
java -Xmx7G -Dtests.run.test=${RUN} -jar target/TriplestoreBenchmarks-1.0.0.jar
killall java
killall virtuoso-t
sleep 3
done
