#!/bin/bash
## Created by Muhammad Ghufran in March 2022

SECONDS=0
echo "<===============Cronjob running datetime is: $(date)===============>"
success_status=0
app_version="v2"
bandwidth_consumption_logs_path="/home/ubuntu/scripts/bandwidth/"
cd ${bandwidth_consumption_logs_path} || exit

# Syncing command to fetch all logs to the server
# If parallel GNU install you can utilize it by running the command below, else use whatever seems feasible.
# Five parallel jobs, syncing and deleting s3 bucket to local after fetch.

parallel -j5 --keep-order :::: regions_s3_sync.cmds
server_fetch_and_delete_status=${?}

# Syncing command to fetch all logs to the server requiring location constraints
parallel -j2 --keep-order :::: loc_cons_regions_s3_sync.cmds

while read -r bucket_logs;
do
	# shellcheck disable=SC2086
	aws s3 rm ${bucket_logs} --recursive
done < loc_cons_regions_logs.s3

echo "Fetch and Delete status is: ${server_fetch_and_delete_status}"

# Giving some rest to server.
echo "It was a tough task to download all region logs simultaneously, Taking some rest Mate..."
sleep 5s
echo "I'm back, lets go..."


# filtering the logs that have 'x-user_id' with GET request and moving those files
# into the filtered_logs folder for the parsing process.
[ -d filtered_logs ] && rm -r filtered_logs/* || mkdir -p filtered_logs
echo "Filtering the logs..."
moving_status=1

for dir in ./all_region_logs/*/
do
	echo "Directory found!, Scanning..."
	if [[ $(grep -Rl "x-user_id" "${dir}" | wc -l) -gt 0 ]];
	then
		grep -Rl "x-user_id" "${dir}" | xargs -i mv {} filtered_logs/
		echo "Scanned ${dir} and moved to filtered_logs/"
	else
		echo "No logs found in the ${dir} having <x-user_id>, which means no logs for bandwidth consumption, going for next directory scan."
	fi
	moving_status=${?}
done

echo "Moving filtered files status is: ${moving_status}"



no_of_logs=$(find filtered_logs/ -type f -exec echo Found file {} \; | wc -l)
echo "The number of logs in the filtered logs are: ${no_of_logs}"

if [[ ${no_of_logs} -gt 0 ]];
then
	# replacing x-user_id with user_id i.e. 'x-' is s3 query token
	if [ "${moving_status}" == "${success_status}" ];
	then
		sed -i 's/x-user_id/user_id/g' filtered_logs/*
		replacing_status=${?} 
		echo "Replacing status is: ${replacing_status}"
	fi
	# Parsing all logs with respect to the Env and saving them into (dev/prod) csv file.
	/usr/bin/python2.7 parseS3Logs.py "${PWD}"

	parsing_status=${?}
	echo "Parsing logs status is: ${parsing_status}"
	if [ "${parsing_status}" == "${success_status}" ];
	then
		echo "OMG, Parsing task was a great journey..."
		
		# Syncing bucket with local csv generated files.
		echo "syncing the server with s3 bucket"
		for csv in ./*.csv
		do
			file_name=$(basename "$csv")
    		environment=$(echo "${file_name}" | cut -f1 -d'-')
    		S3Bucket="s3://${app_version:1}-${environment}-bandwidth-consumption-logs-csv/"

			# shellcheck disable=SC2086
			s3cmd sync ${csv} ${S3Bucket} --stats
		done

		s3_sync_status=${?}
		echo "S3 fetch status is: ${s3_sync_status}"
		if [ "${s3_sync_status}" == "${success_status}" ];
		then
			echo "Cleaning Server..."
			rm ./*.csv
	
			remove_csv_status=${?}
			echo "Remove server csv status is: ${remove_csv_status}"
	
			# Removing all logs recursively on server
			rm -rf all_region_logs/

			remove_logs_status=${?}
			echo "Remove server logs status is: ${remove_logs_status}"
		else
			echo "No files synced to s3 bucket."
		fi
	else
		echo "There is some occurred in parsing the logs or syncing logs to the s3 bucket."
	fi
else
	echo "No Logs found here to be parsed!, Removing all logs..."
	rm -rf all_region_logs/
fi

truncate --size=1G "${PWD}"/../logs/cron_bandwidth_consumptions_log.txt

duration=${SECONDS}
echo "$((duration / 60)) minutes and $((duration % 60)) seconds taken to run the script."
