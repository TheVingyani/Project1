install mysql (set local host password)
Cmd -> mysql -u root -p 
Passwd: root
install dbeaver
set mysql path in system env
set python path in sys env
connect local mysql database using dbeaver
try local mysql for practice 
create database using following cmds
Create database demo; (your choice) 
use demo;
source create-biokg-base.sql (importing full tables from biokg database)
exit;
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



