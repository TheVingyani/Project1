#!/bin/bash


create_schema_only(){
mysqldump --defaults-file=./tcrd.my.cnf --single-transaction --no-data biokg | sed 's/ AUTO_INCREMENT=[0-9]*\b//g' > create-TCRDEV.sql
}
create_schema_only
