#!/bin/bash
db_host='127.0.0.1'
db_user='root'
db_port='3306'
db_pass='123321'
db_name='promotion_db'
table='t_trade_promotion'

exec_time=`date +%Y%m%d-%H%M%S`
base_dir="/tmp/tmp123"
back_dir="/tmp/tmp123"
update_file="update_sql_${exec_time}.txt"
backup_file=${table}_${exec_time}.sql
log_file="update_${table}_${exec_time}.log"

chunk_num=0
chunk_size=50000

fifo_path="${base_dir}/${update_file}.fifo"
load_file="${base_dir}/${update_file}.load"



mkdir $back_dir 2>>/dev/null
mkdir $base_dir 2>>/dev/null
echo "" >${base_dir}/${log_file}
chown  -R mysql.mysql $base_dir

update_sql="select concat('update $table set sale_channel = (sale_channel | 512)  where id=',id,';') into outfile '${base_dir}/${update_file}' from $table  where enabled=1 AND enabled_to > NOW()"

mysql -h$db_host  -u${db_user} -p${db_pass} -P$db_port --database=${db_name} -N -e "${update_sql}" 2>> ${base_dir}/${log_file}

##backup
mysqldump -h$db_host  -u${db_user} -p${db_pass} -P$db_port ${db_name} ${table}  --single-transaction --master-data=2 > ${back_dir}/${backup_file}

if [[ $? != 0 ]];then
    echo -e "backup failed \n"
    exit 1;
fi

##read file with chunk_size 
pt-fifo-split --force --fifo ${fifo_path} --lines ${chunk_size} ${base_dir}/${update_file} &
sleep 10

##update
while [ -e ${fifo_path} ]
do
    chunk_num=$((${chunk_num}+1))
    echo -e "----------fifo file exists! chunk number is: ${chunk_num}.----------\n" >> ${base_dir}/${log_file}
    echo -e "----------fifo file exists! chunk number is: ${chunk_num}.----------\n"
    cat ${fifo_path} > ${load_file}
    mysql -h$db_host  -u${db_user} -p${db_pass} -P$db_port --database=${db_name} < ${load_file}  2>> ${base_dir}/${log_file}
    if [[ $? != 0 ]];then
        echo -e "\033[31m----------ERROR: update chunk  with ERROR at `date +%Y%m%d-%H%M%S`!!!! chunk_num is: ${chunk_num}.----------\033[0m" >> ${base_dir}/${log_file}
	echo -e "\033[31m----------ERROR: update chunk  with ERROR at `date +%Y%m%d-%H%M%S`!!!! chunk_num is: ${chunk_num}.----------\033[0m"
        echo "---------------------------------------------------------------------------" >> ${base_dir}/${log_file}
        cp ${load_file} ${load_file}_failed
    else
         echo -e "----------update chunk completed successfully! chunk number is: ${chunk_num}.----------\n" >> ${base_dir}/${log_file}
         echo -e "----------update chunk completed successfully! chunk number is: ${chunk_num}.----------\n" 
    fi
    sleep 10
done

echo -e "==========update data of table ${table}  completed successfully at `date +%Y%m%d-%H%M%S`.==========\n\n" 
echo -e "==========update data of table ${table}  completed successfully at `date +%Y%m%d-%H%M%S`.==========\n\n" >> ${base_dir}/${log_file}
