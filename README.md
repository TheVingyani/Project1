<h1 align="center">Hi 👋, Welcome To Project 1</h1>
<p align="left"> <img src="https://komarev.com/ghpvc/?username=thevingyani&label=Profile%20views&color=0e75b6&style=flat" alt="thevingyani" /> </p>
<h3 align="left">Languages and Tools:</h3>
<p align="left"> <a href="https://www.mysql.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/mysql/mysql-original-wordmark.svg" alt="mysql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> </p>



















Install Mysql on your system.

https://github.com/TheVingyani/Project1/blob/main/4.1-How-to-install-MySQL.pdf 


Cmd -> mysql -u root -p 
Passwd: root

create database using following cmds

 Create database demo; (your choice) 

  use demo;

   source create-biokg-base.sql (importing full tables)

exit;

install dbeaver

set mysql path in system env
set python path in sys env

connect local mysql database using dbeaver
 

download the data and load the from data folder
for mondo - https://purl.obolibrary.org/obo/mondo.obo


python ./load-Mondo.py --dbhost localhost --dbname demo --pwfile=./demo_pass --dbuser root 

python ./load-DO.py --dbhost localhost --dbname demo2 --pwfile=./demo_pass --dbuser root

python ./load-RDO.py --dbhost localhost --dbname demo2 --pwfile=./demo_pass --dbuser root

enter into mysql
mysql -u root -p
password: root

show databases;
use demo;

SELECT * FROM mondo ORDER BY mondoid DESC LIMIT 10



