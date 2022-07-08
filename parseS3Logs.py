#!/usr/bin/python
# Created by Muhammad Ghufran in March-2022
# parsing code: https://github.com/mg065/AWS-s3-bucket-access-logs

import csv
import os
import re
import dateutil
import pandas as pd
from urlparse import urlparse
import uuid
import sys
from glob import glob

working_env_path = sys.argv[1]
log_path = '{0}/filtered_logs/'.format(working_env_path)

# array for different environment
dev_log_entries = []
prod_log_entries = []
no_of_records = 0
for log in os.listdir(log_path):
    r = csv.reader(open(log_path + log), delimiter=' ', quotechar='"')
    for i in r:
        i[2] = i[2] + ' ' + i[3]  # repair date field
        del i[3]
        try:
            if i[8] == '-' or i[15] == '-':
                print "Skipping...\n Response was not entertained"

            elif len(i[8].split(' ')) > 1 and len(i[15].split('//')) > 1:
                request = i[8].split(' ')[0]
                url = i[8].split(' ')[1]
                response_status = i[9]
                environment = i[15].split('//')[1].split('.')[0]
                conditions_for_bandwidth_consumption = [(request == 'GET'), (response_status == '200')]
                if all(conditions_for_bandwidth_consumption):
                    if len(url.split('?')) > 1:
                        no_of_records += 1
                        if 'user_id=' in url.split('?')[1]:
                            user = url.split('&')[0]
                            user_id = user.split('=')[1]
                            if str(user) == "undefined":
                                user_id = 1
                                # print "user_id=============>", user_id
                            i.append(user_id)
                        else:
                            i.append(0)

                        if 'project_id=' in url.split('?')[1]:
                            project = url.split('&')[1]
                            project_id = project.split('=')[1]
                            if str(project_id) == "undefined":
                                project_id = 1
                            # print "project_alphaid/session_token============>", project_id
                            i.append(project_id)
                        else:
                            i.append(0)

                        if 'company_id=' in url.split('?')[1]:
                            company = url.split('&')[2].split('?')[0].split('/')[0]
                            company_id = company.split('=')[1]
                            if str(company_id) == "undefined":
                                company_id = 1
                            # print "company_id=============>", company_id
                            i.append(company_id)
                        else:
                            i.append(0)

                        if 'guest_user=' in url.split('?')[1]:
                            guest_user = url.split('&')[3].split('?')[0].split('/')[0]
                            guest_user_id = guest_user.split('=')[1]
                            if str(guest_user_id) == "undefined":
                                guest_user_id = 1
                            # print "guest_user_id=============>", guest_user_id
                            i.append(guest_user_id)
                        else:
                            i.append(0)

                        if 'role=' in url.split('?')[1]:
                            role = url.split('&')[4].split('?')[0].split('/')[0]
                            user_role_name = role.split('=')[1]
                            if str(user_role_name) == "undefined":
                                user_role_name = 1
                            # print "user_role_name=============>", user_role_name
                            i.append(user_role_name)
                        else:
                            i.append("Nothing")

                        if 'state_id=' in url.split('?')[1]:
                            state = url.split('&')[5].split('?')[0].split('/')[0]
                            state_id = state.split('=')[1]
                            if str(state_id) == "undefined":
                                state_id = 1
                            # print "state_id=============>", state_id
                            i.append(state_id)
                        else:
                            i.append(0)

                        if environment == 'vision':
                            prod_log_entries.append(i)
                        else:
                            dev_log_entries.append(i)

            else:
                print "skipping...\n Some Error Occurred"

        except Exception as e:
            print e.message

print 'Parsed records count are:\n', no_of_records
# format: http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
columns = ['Bucket_Owner', 'Bucket', 'Time', 'Remote_IP', 'Requester',
           'Request_ID', 'Operation', 'Key', 'Request_URI', 'HTTP_status',
           'Error_Code', 'Bytes_Sent', 'Object_Size', 'Total_Time',
           'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id',
           'Host_Id', 'Signature_Version', 'Cipher_Suite', 'Authentication_Type',
           'Host_Header', 'TLS_Version', 'Access_Point_ARN', 'User_id',
           'Project_id', 'Company_id', 'Guest_User', 'User_Role', 'State_id']

if len(dev_log_entries) > 0:
    if len(dev_log_entries[0]) == len(columns):
        print "Everything is fine here after parsing the logs."
    else:
        print "Columns and and log entries assertion issue"

# dictionary for creating multiple CSV files with respect to the environment
all_log_entries = {'dev': dev_log_entries, 'prod': prod_log_entries}

for log_key, log_entries in all_log_entries.iteritems():
    if len(log_entries) > 0:
        df = pd.DataFrame(log_entries, columns=columns)
        df.Key = df.Key.apply(lambda x: re.sub(
            'index\.html',
            '',
            x.split('/')[0]) if x == x and '{0}/filtered_logs/'.format(working_env_path) not in x.split('/')[
            0] else None)
        df = df[df.Bytes_Sent != '-']
        df = df[df.Key != '-']
        df.Time = df.Time.map(lambda x: x[x.find('[') + 1:x.find(' ')])
        df.Time = df.Time.map(lambda x: re.sub(':', ' ', x, 1))
        df.Time = df.Time.apply(dateutil.parser.parse)
        df['Date'] = df.Time.apply(lambda x: x.strftime('%m-%d-%Y'))
        df.Referrer = df.Referrer.apply(lambda x: urlparse(x).hostname if x == x else None)
        df = df[df.Key.notnull()]
        df = df[df.Bytes_Sent.notnull()]
        del df['Bucket_Owner']
        del df['Bucket']
        del df['Remote_IP']
        del df['Requester']
        del df['Request_ID']
        del df['Operation']
        del df['Key']
        del df['Request_URI']
        del df['HTTP_status']
        del df['Error_Code']
        del df['Object_Size']
        del df['Total_Time']
        del df['Turn_Around_Time']
        del df['Referrer']
        del df['User_Agent']
        del df['Version_Id']
        del df['Host_Id']
        del df['Signature_Version']
        del df['Cipher_Suite']
        del df['Authentication_Type']
        del df['Host_Header']
        del df['TLS_Version']
        del df['Access_Point_ARN']

        current_date = str(pd.datetime.now().date())

        if len(df) > 0:
            print "Dataframe rows===>", len(df)
            df.to_csv(
                '{0}/'.format(working_env_path) + log_key + '-' + current_date + '-' + uuid.uuid4().hex[
                                                                                                  :7].upper() + '.csv',
                index=False)
            file_name = glob(r'{0}/'.format(working_env_path) + log_key + '-*.csv')[0]
            print file_name
            df = pd.read_csv(file_name)
            total_bytes_rows = \
                df.groupby(['User_id', 'Project_id', 'Company_id', 'Guest_User', 'User_Role', 'State_id', 'Date'])[
                    'Bytes_Sent'].sum().reset_index()
            total_bytes_rows.to_csv(file_name, index=False)

        else:
            print 'No csv file created!'
