# install java8
sudo add-apt-repository ppa:webupd8team/java
sudo apt update; sudo apt install oracle-java8-installer
sudo apt install oracle-java8-set-default

# install mysql-server
sudo apt-get update
sudo apt-get install mysql-server

# create table
mysql -uroot -proot < setup.sql