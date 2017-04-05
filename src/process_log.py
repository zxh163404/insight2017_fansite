#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Xiaohui(Leo) Zhong
"""

#%%

        
import time
from datetime import datetime, timedelta
import pandas as pd
import sys
from collections import deque

#This the datetime format of the timestamp in the log.txt
time_format = '%d/%b/%Y:%H:%M:%S'

t0 = time.time()

#The file_input specify filename of inputs.
file_input = sys.argv[1] 

#hosts_output specify the filename to write in descending order the top 10 most active hosts/IP addresses that have accessed the site
hosts_output = sys.argv[2] 

#hours_output specify the filename to write in descending order the site’s 10 busiest (i.e. most frequently visited) 60-minute period
hours_output = sys.argv[3]

#resources_output specify the filename to write in descending order the top 10 resources on the site that consume the most bandwidth
resources_output = sys.argv[4]

#The file_blocked specify the filename to write the blocked attempts.
file_blocked = sys.argv[5]

with open(file_input, "r", encoding="latin-1") as f_input, open(file_blocked, 'w') as f_out:
    #Initialize
    host = {}    #key is either hostname or IP address, and value is the number of times they have accessed the site
    resource = {}   #key is the resources fo the site, and value is the bandwidth consumption        
    startof_60minute = deque()     #list the end of all the 60-minutes period
    N_visits = deque()     #number of visits correponding to each of the 60-minutes period
    N_visits_sorted = []    #the 10 most number of visits among all the 60-minute time periods. 
    startof_60minute_sorted = []  #the top 10 busiest 60-minute time period.
    N_visits_max = 0;

    #record all the failed logins over a consective 20 seconds
    #Index is the hostname of IP address which has made failed login attempt    
    #Column 1 ("N_failed") is the number of failed attempts over 20 seconds, ranging from 1 to 3
    #Column 2 ("datetime_login") is the datetime
    #If the value of Column 1 is 1 or 2, means last login is a failed attempt 
    #and within 20 seconds from the first failed login; Column 2 is the datetime + 20 seconds
    #when first failed login attempt happens 
    #If the value of Column 1 is 3, means the three consecutive failed login attempts happened and the host was blocked;
    #Column 2 is the "datetime when the third failed login attempt + 5 minutes"

    failed_logins = pd.DataFrame({'N_failed': [], 'datetime_failedlogin': []})

    for line in f_input:
        
        ###Start to search for the end of the hostname or IP address
        #IP address ranges from 0.0.0.0 to 255.255.255.255, so if it is IP address, 
        #then the search can start from the 8. Also, most of hostnames are longer than 7.
        host_end = line.find(' - - ',8)
               
        if host_end < 0:
            #Sometimes, the hostname is shorter than 7, e.g. modem1, then we need
            #to start to search for the end of hostname or IP address from the begining of the line
            host_end = line.find(' - - ')
            if host_end < 0:
                #No hostname or IP address is found
                continue
        
        #Get the hostname or IP address which made this failed login attempt
        host_temp = line[:host_end]
            
        ''
        if host_temp in host.keys():
            #the hostname or IP address is stored, add the number of visits by 1
            host[host_temp] = host[host_temp] + 1        
        else:
            #the hostname or IP address is not stored, initialize the number of visits by 1            
            host[host_temp] = 1
            
        ###End for finding hosts
        ''

        ###record the number of visists for each of the 60-minutes period
        #Get the datetime of this request        
        time_temp = datetime.strptime(line[host_end + 6:host_end + 26], time_format)                  
        
        ''
        if not startof_60minute:
            #Initialize startof_60minute and N_visits
            startof_60minute.append(time_temp)
            N_visits.append(1)                
            for n in range(1,3600):
                startof_60minute.append(time_temp + timedelta(seconds = n))
                N_visits.append(0)            
            
        else:
            diff_seconds = int((time_temp - startof_60minute[0]).total_seconds())
                        
            if diff_seconds >= 3600:
                #This request happens more than one hour after the oldest one stored in startof_60minute                 
                
                #The flag_sort indicate if it is necessary to resort the N_visits_sorted and startof_60minute_sorted 
                flag_sort = 0
                
                #This request happens more than one hour after the oldest request in stored in startof_60minute_sorted
                
                #Get the number of items that will be dequeued and enqueued                      
                N_temp = diff_seconds - 3600 + 1
                
                if N_temp <= 3600:
                    N_changes = N_temp
                else:
                    #Set the number of items to be dequeued to be 3600 so that the number 
                    #of items in the queue stays at 3600
                    N_changes = 3600
                    
                for n in range(N_changes):
                    #Dequeue all the N_temp items from the startof_60minute and N_visits,
                    # and append them to startof_60minute_sorted and N_visits_sorted.     
                    if N_visits[0] > N_visits_max:                        
                        startof_60minute_sorted.append(startof_60minute.popleft())
                        N_visits_sorted.append(N_visits.popleft())   
                        
                        #New items are appended
                        flag_sort = 1
                        
                    else:
                        startof_60minute.popleft()
                        N_visits.popleft()
                        
                    #Enqueue the most recent time  
                    startof_60minute.append(time_temp + timedelta(seconds = - N_changes + n + 1))
                    N_visits.append(0)

                #Check if it is necessary to resort the N_visits_sorted and startof_60minute_sorted  
                if flag_sort == 1:
                    #Resort the N_visits and only keep in the desending order the 10 most number of visits among all the 60-minute time periods. 
                    
                    N_visits_sorted_temp = sorted(enumerate(N_visits_sorted), key=lambda x:x[1], reverse = True)[:10]
                    N_visits_sorted = [x[1] for x in N_visits_sorted_temp]                   
                    #Get the top 10 busiest 60-minute time period. 
                    startof_60minute_sorted = [startof_60minute_sorted[x[0]] for x in N_visits_sorted_temp]        

                    N_visits_max = N_visits_sorted[0]
                    
                #All the values in N_visits will be added by one
                for n in range(3600):
                    N_visits[n] = N_visits[n] + 1
              
            else:
                #This request happens more than one hour after the oldest one stored in startof_60minute
                N_changes = diff_seconds + 1
                for n in range(0,N_changes):
                    N_visits[n] = N_visits[n] + 1
                      
        ###End for finding the number of visists for 60-minute period
        ''
        
        ###Start to search for the begining of the names of resources from 35 characters after host_end based on the fixed data format ( '- - [DD/MON/YYYY:HH:MM:SS -0400] "')
        resource_start = line.find('/', host_end+35)    
        
        bytes_start = line.find('" ',resource_start+1) - 1
        bytes_sent = line[bytes_start+7:]
        if '-' in bytes_sent:
            #no bytes were sent, thus no need to continue to record the bandwidth consumption
            bytes_sent = 0
        else:
            bytes_sent = int(bytes_sent)
            #parse the 
            resource_end = line.find(' ',resource_start,bytes_start)
            if resource_end < 0:
                resource_end = bytes_start + 1
            if line[resource_start:resource_end] in resource.keys():
                #The resource of the site has been accessed before, add bandwidth consumption 
                resource[line[resource_start:resource_end]] = resource[line[resource_start:resource_end]] + bytes_sent
            else:
                #The resource of the site is accessed for the first time, initialize with bandwidth consumption 
                resource[line[resource_start:resource_end]] = bytes_sent
        ###End for find resources
        ''
        
        ###Start to check for feature 4
        if line[resource_start:resource_end] != '/login':
            #This is not a login attempt
            
            #Check if the host made any failed login attempts before
            if host_temp in failed_logins.index:
                #The host made failed login attempts before 
                
                #Check if the host is still blocked at time_temp
                if failed_logins.loc[host_temp,'N_failed'] == 3 and time_temp <= failed_logins.loc[host_temp,'datetime_failedlogin']: 
                     #The host is still blocked
                     #Write this attempts to the blcked.txt file
                     f_out.write(line)                                   
        
        elif line[resource_start:resource_end] == '/login':
            #This is a login attempt
            
            #Check whether this login attempt is failed or successful
            if line[line.find('" ',resource_end) + 2:line.find('" ',resource_end) + 3] == '4':
                #A failed login                    

                #Check if the host is in the list of hosts who made failed logins
                if host_temp in failed_logins.index:
                    #The corresponding host is in ths list                
                        
                    if failed_logins.loc[host_temp,'N_failed'] == 1 and time_temp < failed_logins.loc[host_temp,'datetime_failedlogin']:
                        #The last one login before this failed attempt was failed                                             
                        #The second consecutive failed attempt is within the 20 seconds window   
                        #Note that "<=" is not used because if they are equal, 
                        #then even if the next attempt was failed, it would not be
                        #within the 20 seconds window
                        #Change the number of consecutive failed attempts from 1 to 2
                        failed_logins.loc[host_temp,'N_failed'] = 2

                    elif failed_logins.loc[host_temp,'N_failed'] == 2 and time_temp <= failed_logins.loc[host_temp,'datetime_failedlogin']:
                        #The last two login attempts before this failed attempt were failed within the 20 seconds window                        
                        #The third consecutive failed attempt is within the 20 seconds window
                        #Change the number of consecutive failed attempts from 2 to 3
                        failed_logins.loc[host_temp,'N_failed'] = 3
                        #Block any of the attempts for 5 minutes from time_temp.                            
                        failed_logins.loc[host_temp,'datetime_failedlogin'] = time_temp + timedelta(minutes=5)                          

                    elif failed_logins.loc[host_temp,'N_failed'] == 3 and time_temp <= failed_logins.loc[host_temp,'datetime_failedlogin']:                         
                        #The host was blocked                        
                        #The host is still blocked
                        #Write this attempts to the blcked.txt file
                        f_out.write(line)
                     
                    else:                        
                        #Reset the number of failed login attempts to 1 and
                        #update the time of failed login attempt to be time_temp + 20 seconds
                        failed_logins.loc[host_temp,'N_failed'] = 1                        
                        failed_logins.loc[host_temp,'datetime_failedlogin'] = time_temp + timedelta(seconds=20)
                            
                        
                else:
                    #The host made failed login attempt for the first time
                    failed_logins.loc[host_temp] = [1, time_temp + timedelta(seconds=20)]                    

            else:
                 #A successful login
                 
                 #Check if the host made any failed login attempts before 
                 if host_temp in failed_logins.index:
                     
                     #Check if the host is still blocked at time_temp
                     if failed_logins.loc[host_temp,'N_failed'] == 3 and time_temp <= failed_logins.loc[host_temp,'datetime_failedlogin']:                         
                         #The host is still blocked
                         #Write this attempt to the blcked.txt file
                         f_out.write(line)
                             
                     else:
                         #It has been either more than 5 minute since the host was blocked
                         #or it was not blocked less than 3 times in a row
                         #Remove this host from the list of hosts who made failed logins
                         failed_logins = failed_logins.drop([host_temp])
                
        ###End for find the failed attempts
        ''
                 
        
t1 = time.time()
print(t1 - t0, "seconds process time\n")
#%%
t0 = time.time()
#Sort in the descending order the active host
#Get the top 10 active hosts
host_sorted = sorted(host.items(), key = lambda x:x[1], reverse = True)[:10]

#Get the top 10 resources of the site that has consume most bandwidth
resource_sorted = sorted(resource.items(), key=lambda x:x[1], reverse = True)[:10]

#Dequeue all the items from the startof_60minute and N_visits, and append them 
#to startof_60minute_sorted and N_visits_sorted.
for n in range(len(startof_60minute)):
    startof_60minute_sorted.append(startof_60minute.popleft())
    N_visits_sorted.append(N_visits.popleft())                    

#Resort the N_visits and only keep in the desending order the 10 most number of visits among all the 60-minute time periods. 
N_visits_sorted_temp = sorted(enumerate(N_visits_sorted), key=lambda x:x[1], reverse = True)[:10]
N_visits_sorted = [x[1] for x in N_visits_sorted_temp]                   
#Get the top 10 busiest 60-minute time period. 
startof_60minute_sorted = [startof_60minute_sorted[x[0]] for x in N_visits_sorted_temp]
                         
t2 = time.time()
print(t2 - t0, "seconds sorting time\n")

#%% Write data into files.
t0 = time.time()

#Start to write data
with open(hosts_output, 'w') as out_hosts:
    for i in range(len(host_sorted)):
        out_hosts.write(host_sorted[i][0] + ',' + str(host_sorted[i][1]) + '\n')    
        
with open(resources_output, 'w') as out_resources:
    for i in range(len(resource_sorted)):    
            out_resources.write(resource_sorted[i][0] + '\n')
            
with open(hours_output, 'w') as out_hours:
    for i in range(len(N_visits_sorted)):    
        out_hours.write(startof_60minute_sorted[i].strftime(time_format) + ' -0400,' + str(N_visits_sorted[i]) + '\n')

t3 = time.time()
print(t3 - t0, "seconds writing time\n")
