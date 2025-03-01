#!/bin/bash

#=====================================#
### RUN OS LINUX SERVER 
### DATABASE USING POSTGRESQL 
#=====================================#


#--- STOP AND START TOMCAT SERVICE
checkService=$(ps -p 1 -o comm=)
if [ "$checkService" == "systemd" ]; then
    echo "Stop Tomcat"
    systemctl stop tomcat
    sleep 10

    echo "Start Tomcat"
    systemctl start tomcat
    sleep 10
	echo "Done"
elif [ "$checkService" == "init" ]; then
    echo "Stop Tomcat"
    /etc/init.d/tomcat stop
    sleep 10

    echo "Start Tomcat"
    /etc/init.d/tomcat start
    sleep 10
	echo "Done"
else
    echo "Stop Tomcat"
    systemctl stop tomcat.service
    sleep 10

    echo "Start Tomcat"
    systemctl start tomcat.service
    sleep 10
	echo "Done"
fi


#--- SETUP VARIABLE
vDate=$(date +"%d-%m-%Y_%0k-%M")
vYesterday=$(date -d "${date}-1 day" +"%d-%m-%Y")
vYesterday2=$(date -d "${date}-1 day" +"%Y-%m-%d")
vDB=db_name
vUser=db_user
vLocalhost=db_ip
vPass=db_pass

PGPASSWORD="$vPass" psql -U "$vUser" -h "$vLocalhost" "$vDB" << EOF
copy (select column_name from table1) to '/path/to/address.txt'
EOF

PGPASSWORD="$vPass" psql -U "$vUser" -h "$vLocalhost" "$vDB" << EOF
copy (select column_name from table2) to '/path/to/city.txt'
EOF

address=($(</path/to/address.txt))
city=($(</path/to/city.txt))

FILENAME="backup_$address$city-$vDB-$vDate"

#--- CREATE LOGFILE
LOGFILE="Log_backupdb-$address-$city.txt"

#--- BACKUP PROCESS
run_process=/path/to/store/backup/
if [ -d $run_process ]; then
   echo "$vDate : Folder found" >> /path/to/$LOGFILE
else
   mkdir /path/to/store
   mkdir /path/to/store/backup
fi


if [ -d "$run_process" ]; then
    pg_dump -U "$vUser" -h "$vLocalhost" "$vDB" > "$run_process/$FILENAME"
    gzip -n "$run_process/$FILENAME"
    
    check_gzip=$?
    if [ "$check_gzip" -ne 0 ]; then
        echo "$vDate : Failed to compress backup, exit code = $check_gzip" >> /path/to/$LOGFILE
    else
        echo "$vDate : Successfully compressed backup" >> /path/to/$LOGFILE
    fi

    if [ -f "$run_process/$FILENAME.gz" ]; then
        check_size=$(stat -c%s "$run_process/$FILENAME.gz")
        size_now=$(ls -lh "$run_process/$FILENAME.gz" | awk '{ print $5 }')

        if [ "$check_size" -gt 0 ]; then
            echo "$vDate : Backup successful, file size: $size_now" >> /path/to/$LOGFILE
        else
            echo "$vDate : Backup failed, file is empty!" >> /path/to/$LOGFILE
        fi
    else
        echo "$vDate : Backup file not found!" >> /path/to/$LOGFILE
    fi

    echo "Delete backups older than 7 days"
    find "$run_process" -type f -name "*.gz" -mtime +7 -exec rm -f {} \;
    echo "$vDate : Old backups (older than 7 days) deleted" >> /path/to/$LOGFILE
else
    echo "$vDate : Backup directory $run_process not found!" >> /path/to/$LOGFILE
fi

exit