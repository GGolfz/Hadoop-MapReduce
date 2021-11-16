mkdir balanceDetector && javac -classpath hadoop-core-1.2.1.jar -d balanceDetector BalanceDetector.java && jar -cvf BalanceDetector.jar -C balanceDetector/ . && rm -rf balanceDetector
hadoop jar BalanceDetector.jar BalanceDetector input/data.csv output
hadoop fs -getmerge -nl output error_balance.txt && hadoop fs -rm -R output && hadoop fs -mkdir output && hadoop fs -put error_balance.txt output/error_balance.txt && rm error_balance.txt
