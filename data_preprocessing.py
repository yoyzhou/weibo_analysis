#!/usr/bin/env python
#coding=utf8

'''
Created on Apr 9, 2013

@author: yoyzhou
'''

import os
from os.path import join
from mimify import File

def concatenateAll(following_files_folder):
    '''
    Concatenate all weibo info resided in following files, in which each line presents a following user.
    Function needs to eliminate duplications
    '''
    following_user_all = open('following_user_all.txt', 'w')
    uids = set()
    
    for root, _, files in os.walk(following_files_folder):
        for following_file in files:
            with open(join(root, following_file)) as followings:
                for line in followings:
                    if line.strip():
                        try:
                            uid = line.split('*_*')[0].split(':')[1]
                            if not uid in uids:
                                uids.add(uid)
                                following_user_all.write(line)
                        except:
                            print line + following_file
                            pass
                            
    with open('following_uid_all.txt', 'w') as f_all:
        f_all.write('\n'.join(uids))
    
    following_user_all.flush()
    following_user_all.close()

def aggregate_following_ntk(following_files_folder):
    '''
    Aggregate user following network from all the following files, note each file contains all the followings of
    one individual user, whose uid is the prefix of file name. like xxxxxxxxxx_his_followings.txt where xxxxxxxxxx 
    is the user uid. 
    '''
   
    following_ntk = open('following_ntk.txt', 'w')
    
    for root, _, files in os.walk(following_files_folder):
        for following_file in files:
            user_uid = following_file.split('_')[0] #the prefix is user uid
            with open(join(root, following_file)) as follows_file:
                followings = []
                for line in follows_file:
                    if line.strip():
                        try:
                            uid = line.split('*_*')[0].split(':')[1]
                            followings.append(uid)
                        except:
                            print line + following_file
                            pass
                following_ntk.write(user_uid + ':' + ','.join(followings) + '\n')
                            
    following_ntk.flush()
    following_ntk.close()
    

def extract_cofollowed_user_info():
    """
    extract user info according to the mahout cluster results, add concatenate cluster group into user info
    file description:
    fntk.csv - contains all the following network edges
    following_user_all.csv - contains all the users with weibo account id and user name, etc.
    results.csv - contains mahout cluster results with account id and cluster id 
    """
    cluster_users_info = open("cluster_users_info.csv", 'w')
    cluster_users_fntk =  open("cluster_users_fntk.csv", 'w')
    
    cluster_results = {}
    #cache the cluster results
    with open('results.csv', 'r') as results:
        for line in results:
            fields = line.split('\t')
            cluster_results[fields[0].strip()] = fields[1]
    
    #write out records in fntk.csv if nodes are in cluster results
    cluster_users_fntk.write('Source\tTarget\tType\n')
    with open('fntk.csv', 'r') as fntk:
        for line in fntk:
            fields = line.split('\t')
            if fields[0].strip() in cluster_results.keys() or  fields[1].strip() in cluster_results.keys() :
                cluster_users_fntk.write(line)
               
    #write out records in following_user_all.csv if nodes are in cluster results
    cluster_users_info.write('Nodes\tId\tLabel\tSex\tClusterId\n')
    with open('following_user_all.csv', 'r') as fu:
        for line in fu:
            fields = line.split('\t')
            if fields[0].strip() in cluster_results.keys() :
                cluster_users_info.write(line.strip() + '\t' + cluster_results[fields[0].strip()])
    
    
    cluster_users_info.flush()
    cluster_users_info.close()
    cluster_users_fntk.flush()
    cluster_users_fntk.close()
    
if __name__ == '__main__':
    #concatenateAll('D:\\dataset\\weibo\\data\\followings')
    #aggregate_following_ntk('D:\\dataset\\weibo\\data\\followings')
    extract_cofollowed_user_info()