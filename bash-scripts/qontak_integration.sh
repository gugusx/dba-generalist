#!/bin/sh

#--- FIND PSQL SERVICES
temp_db=$(ps aux | grep '[p]ostgres*' | awk '{print $(NF)}' | grep -F '/' | grep -iF 'postgresql' | head -n 1)
temp_bin=$(ps aux | grep '[p]ostgres*' | awk '{print $11}' | grep -F '/' | grep -iF 'postgresql' | head -n 1)
cekpostgres=$(echo "$temp_bin" | wc -l)
cekdbpostgres=$(echo "$temp_db" | wc -l)
if [ "$temp_db" != "" ] && [ "$temp_bin" != "" ] && [ $cekpostgres = 1 ] && [ $cekdbpostgres = 1 ] ; then
 TEMP=$(dirname $temp_bin)
else
 TEMP=""
 temp_db=""
fi


#-- SETUP VARIABLE
username="your_username"
password="your_password"
client_id="your_client_id"
client_secret="your_client_secret"

MAX_RETRIES=5
retry_count=0

host="your_ip"
user="your_db_user"
db="db_name"


#--- FUNCTION TO GET TOKEN
get_token() {
    while true; do
        response=$(curl --request POST \
            --url "https://service-chat.qontak.com/oauth/token" \
            --header "Content-Type: application/json" \
            --data "{
                \"username\": \"$username\",
                \"password\": \"$password\",
                \"grant_type\": \"password\",
                \"client_id\": \"$client_id\",
                \"client_secret\": \"$client_secret\"
        }")

        if [ $? -eq 0 ]; then
          echo "Curl request successful"
          break
        else
          retry_count=$((retry_count + 1))
          if [ $retry_count -ge $MAX_RETRIES ]; then
            echo "Max retries reached. Exiting."
            exit 1
          fi
          echo "Curl request failed. Retrying in 5 seconds (attempt $retry_count/$MAX_RETRIES)..."
          sleep 5
        fi
    done
}


echo "CHECK FINISH DATE"
cekfinish=$($TEMP/./psql -h 127.0.0.1 -Uroot pos -qtAc "select date(finish_date)-current_date from table_1 where date(finish_date)-current_date = 3 group by 1")
if [ "$cekfinish" -eq 3 ]; then
  echo "THERE ARE MEMBERS WHO NEED TO BE REMINDED"

#==================================GET TOKEN==================================#
  echo "CHECK TOKEN"
  if [ -f /path/to/token_qontak.txt ]; then
    echo "Token already exist, check expired date"
    expires_in=$(tail -1 /path/to/token_qontak.txt)
    current_timestamp=$(date +%s)
    expiration_timestamp=$((current_timestamp + expires_in))
    expiration_date=$(date -d "@$expiration_timestamp" +"%Y-%m-%d")
    now=$(date +"%Y-%m-%d")

    if [ $(date -d "$now" +%s) -ge $(date -d "$expiration_date" +%s) ]; then
      echo "Token expired, get new token"
      get_token
      access_token=$(echo "$response" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
      expires_in=$(echo "$response" | grep -o '"expires_in":[^,]*' | awk -F':' '{print $2}' | tr -d '[:space:]')
      echo -e "$access_token\n$expires_in" > /path/to/token_qontak.txt
    else
      echo "Not expired yet"
    fi
  else
    echo "TOKEN CANNOT FOUND, GENERATE FIRST TOKEN"
    get_token
    access_token=$(echo "$response" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
    expires_in=$(echo "$response" | grep -o '"expires_in":[^,]*' | awk -F':' '{print $2}' | tr -d '[:space:]')
    echo -e "$access_token\n$expires_in" > /path/to/token_qontak.txt
  fi
#==================================================================================#


#==================================GET WA CHANNEL==================================#
  token_value=$(head -1 /path/to/token_qontak.txt)

  echo "GET WA CHANNEL"
  while true; do
    getchannel=$(curl --request GET \
       --url 'https://service-chat.qontak.com/api/open/v1/integrations?target_channel=wa&limit=10' \
       --header "Authorization: Bearer $token_value")

       if [ $? -eq 0 ]; then
         echo "Curl request successful"
         break
       else
         retry_count=$((retry_count + 1))
         if [ $retry_count -ge $MAX_RETRIES ]; then
           echo "Max retries reached. Exiting."
           exit 1
         fi
         echo "Curl request failed. Retrying in 5 seconds (attempt $retry_count/$MAX_RETRIES)..."
         sleep 5
       fi
  done

  channel_id=$(echo "$getchannel" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
  echo "$channel_id" > /path/to/channel_id.txt
#==================================================================================#


#==================================GET WA TEMPLATE==================================#
  token_value=$(head -1 /path/to/token_qontak.txt)

  echo "GET WA TEMPLATE"
  while true; do
     template_id=$(curl --request GET \
       --url https://service-chat.qontak.com/api/open/v1/templates/whatsapp \
       --header "Authorization: Bearer $token_value" | jq -r '.data[] | select(.name == "1order_care") | .id')

       if [ $? -eq 0 ]; then
         echo "Curl request successful"
         break
       else
         retry_count=$((retry_count + 1))
         if [ $retry_count -ge $MAX_RETRIES ]; then
           echo "Max retries reached. Exiting."
           exit 1
         fi
         echo "Curl request failed. Retrying in 5 seconds (attempt $retry_count/$MAX_RETRIES)..."
         sleep 5
       fi
  done

  echo "$template_id" > /path/to/template_id.txt
#==================================================================================#


#===================================SEND WA MESSAGE OUTBOUNT DIRECT==================================#
  token_value=$(head -1 /path/to/token_qontak.txt)
  channel_id=$(cat /path/to/channel_id.txt)
  template_id=$(cat /path/to/template_id.txt)

  echo "SEND MESSAGE"
  $TEMP/./psql -h "$host" -U "$user" "$db" -tAF "|" -c "SELECT initcap(column_1) as column_1, column_2, to_char(date(value_3),'DD-MM-YYYY') as value_3, string_agg(initcap(column_4), '; ') as column_4, string_agg(quote_literal(column_5), ',') as column_5 FROM (select column_5, column_6, column_1, case when substr(trim(replace(column_2,' ', '')),1,2) = '08' then '628' || substr(column_2,3,20) when substr(trim(replace(column_2,' ','')),1,3) = '+62' then '62' || substr(column_2,4,20) else trim(replace(column_2,' ','')) end as column_2, date_trunc('second', value_3) as value_3, date_trunc('second', finish_date) as  finish_date, i.column_4, date(finish_date)-current_date as remaining_days from table_1 h left join table_2 i on h.item_code = i.item_code where length(column_2) between 10 and 15 group by 1,2,3,4,5,6,7,8) as a WHERE remaining_days = 3 GROUP BY 1,2,3;" | while IFS="|" read -r column_1 column_2 value_3 column_4 column_5
  do

    while true; do
      sendMessage=$(curl --request POST \
      --url https://service-chat.qontak.com/api/open/v1/broadcasts/whatsapp/direct \
      --header "Authorization: Bearer $token_value" \
      --header 'Content-Type: application/json' \
      --data '{
          "to_number": "'"$column_2"'",
          "to_name": "'"$column_1"'",
          "message_template_id": "'"$template_id"'",
          "channel_integration_id": "'"$channel_id"'",
          "language": {
            "code": "id"
          },
          "parameters": {
            "body": [
            {
              "key": "1",
              "value": "value_1",
              "value_text": "'"$column_1"'"
            },
            {
              "key": "2",
              "value": "value_2",
              "value_text": "'"$column_4"'"
            },
            {
              "key": "3",
              "value": "value_3",
              "value_text": "'"$value_3"'"
            }
          ]
        }
      }')

      if [ $? -eq 0 ]; then
        echo "Curl request successful"
        break
      else
        retry_count=$((retry_count + 1))
        if [ $retry_count -ge $MAX_RETRIES ]; then
          echo "Max retries reached. Exiting."
          exit 1
        fi
        echo "Curl request failed. Retrying in 5 seconds (attempt $retry_count/$MAX_RETRIES)..."
        sleep 5
      fi
    done
  log=$(echo "$sendMessage" | grep -o '"status":"[^"]*' | cut -d'"' -f4 | head -1)
  fn=$(echo "$sendMessage" | grep -o '"value_1":"[^"]*' | cut -d'"' -f4)
  echo "$(date +"%d-%m-%Y")", "$log", "$fn" >> /path/to/log_sendMessage.txt

  done
#==================================================================================#
else
   echo "NOTHING TO DO"
fi
