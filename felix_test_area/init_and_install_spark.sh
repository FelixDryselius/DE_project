#!/bin/sh

# This will upgrade the machine
sudo apt-get update -y
sudo apt-get --with-new-pkgs upgrade -y

# This installs Java
sudo apt install openjdk-8-jdk -y

# This downloads Spark
curl -O https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz

# Unpack the file
tar xvf spark-3.2.1-bin-hadoop3.2.tgz

# Remove the compressed file
rm spark-3.2.1-bin-hadoop3.2.tgz

# Move the file to the correct location
sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark

# Check the PATH exists
if [ $SPARK_HOME = "" ]; then
      # Append PATH to .bashrc
      echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
      echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc
fi

# Reload the terminal
source ~/.bashrc
