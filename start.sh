${SPARK_HOME}/bin/spark-submit \
--master local[2] \
--class AR.Main \
--executor-memory 2G \
--driver-memory 2G \
/home/maple/homework/aos/2018National-College-Competition-on-Cloud-Computing/target/Question1-1.0-SNAPSHOT.jar \
"/home/maple/homework/aos/input" \
"/home/maple/homework/aos/output" \
"/home/maple/homework/aos/tmp"
