s3 access logs manipulation DFD: https://miro.com/app/board/uXjVOIfkYUk=/?invite_link_id=669274464745

Steps in bash scripts:

make your path global $ echo "export bandwidth_consumption_logs_path=~/scripts/bandwidth"
Syncing logs of all regions to server machine in parallel/simultaneously.
Deleting all-region logs from the bucket.
Filtering logs by 'x-user_id' and moving them into separate folders.
Replacing 'x-user_id' by 'user_id'
Running a python script to parse the logs and save them into the CSV file with respect to the environment(dev/prod), where Lambda function does the rest of putting records in the database.
Syncing CSV file to s3 bucket (i.e dev-bandwidth-consumtions-logs)
Cleaning work of the server machine by removing all logs and CSV files.

