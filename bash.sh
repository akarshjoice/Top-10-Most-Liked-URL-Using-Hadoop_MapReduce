#!/bin/bash
clear
hadoop fs -rm -r /FacebookTutorial/Output
javac -classpath ${HADOOP_CLASSPATH} -d '/home/hduser/Desktop/FacebookTutorial/tutorial_classes' '/home/hduser/Desktop/FacebookTutorial/ReduceJoin.java'
/opt/jdk/jdk1.8.0_202/bin/jar -cvf firstTutorial.jar -C tutorial_classes/ .
hadoop jar firstTutorial.jar ReduceJoin /FacebookTutorial/Input /FacebookTutorial/Output utterpradesh
hadoop dfs -cat /FacebookTutorial/Output/*
