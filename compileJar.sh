mkdir balanceDetector && javac -classpath hadoop-core-1.2.1.jar -d balanceDetector BalanceDetector.java && jar -cvf BalanceDetector.jar -C balanceDetector/ . && rm -rf balanceDetector
hadoop fs -rm -R output && hadoop jar BalanceDetector.jar BalanceDetector input/data.csv output
