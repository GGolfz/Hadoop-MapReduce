```
sudo adduser hadoop
sudo adduser hadoop sudo
su - hadoop
sudo apt-get update && sudo apt-get -y dist-upgrade && sudo apt-get -y install openjdk-8-jdk-headless
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz && tar xvzf hadoop-3.3.1.tar.gz && mv hadoop-3.3.1 hadoop && rm hadoop-3.3.1.tar.gz
```
`vi ~/hadoop/etc/hadoop/hadoop-env.sh`
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HDFS_NAMENODE_USER="hadoop"
export HDFS_DATANODE_USER="hadoop"
export HDFS_SECONDARYNAMENODE_USER="hadoop"
export YARN_RESOURCEMANAGER_USER="hadoop"
export YARN_NODEMANAGER_USER="hadoop"
export HADOOP_HOME=/home/hadoop/hadoop
```
`source ~/hadoop/etc/hadoop/hadoop-env.sh`