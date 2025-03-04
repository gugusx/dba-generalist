#!/bin/bash

#--- SETUP VARIABLE
lastmonth=$(date -d "${date}-1 month" +%-m)
lastyearmonth=$(date -d "${date}-1 month" +%Y)
currentmonth=$(date +%-m)
currentyear=$(date +%Y)
appname="your_app"

#--- GET IP HOST
ip=$(hostname -I | awk '{print $1;}')
ip_port=your_port


if [ "$(date +%-d)" == "1" ]; then
    for a in "api1" "api2" "api3" "api4"
    do
        echo "API $a"
        stat_sync=$(curl -X POST "http://$ip:$ip_port/$appname/$a?month=$lastmonth&year=$lastyearmonth")
        vDate=$(date +"%Y-%m-%d %H:%M:%m")
        vStatus=$(echo "$stat_sync" | grep -oP '"message":\s*"\K[^"]+')
        echo "$a, $vStatus, $vDate" > /path/to/lastmonth_log_API_$a.txt
    done
elif [ "$(date +%-d)" == "15" ]; then
    for a in "api1" "api2" "api3" "api4"
    do
        echo "API $a"
        stat_sync=$(curl -X POST "http://$ip:$ip_port/$appname/$a?month=$currentmonth&year=$currentyear")
        vDate=$(date +"%Y-%m-%d %H:%M:%m")
        vStatus=$(echo "$stat_sync" | grep -oP '"message":\s*"\K[^"]+')
        echo "$a, $vStatus, $vDate" > /path/to/currentmonth_log_API_$a.txt
    done
else
    echo "Skip"
fi