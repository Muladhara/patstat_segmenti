# -*- coding: utf-8 -*-

# ----
from collections import defaultdict
from itertools import ifilter
from pprint import pprint
import MySQLdb
import re
import os
import sys
import copy
from datetime import datetime ,date
from os.path import exists
import cPickle

sys.path.append(os.getcwd())
from patstat.cleaning import *

sock=os.getenv('MYSQL_SOCK')
pwd=os.getenv('MYSQL_PWD')

db=MySQLdb.connect(user="root",passwd=pwd,db="patstat_segmenti2012",use_unicode=True,unix_socket=sock)

c=db.cursor()
fname = '5_prior.pickle'
if exists("1-"+fname):
    pers,appls = cPickle.load(open("1-"+fname,'r'))
else:
    pers={}
    appls={}
    patents=[]
    c.execute('''SELECT person_id FROM ITA_KNOWN_APPLICANTS''')
    known_applicants=c.fetchall()
    for i,id in enumerate(known_applicants):
        if i % 1000 == 0: print i,'on',len(known_applicants),len(pers),len(appls)
            pers[id]=list()
            c.execute('''SELECT appln_id FROM itaappdb.TLS207_PERS_APPLN where person_id=%s''',id)
            patents=c.fetchall()
            for p in patents:
                pers[id].append(p)
                c.execute('''SELECT prior_appln_id FROM patstat201204.TLS204_APPLN_PRIOR WHERE appln_id=%s''',p)
                priors=c.fetchall()
                c.execute('''SELECT appln_filing_date FROM itaappdb.TLS201_APPLN b where appln_id=%s ''',p)
                pdate = c.fetchone()
                appls[p[0]]={ 'prior': priors, 
                              'date':  pdate}
                
                print "P:",len(pers)
                print "A:",len(appls)
                cPickle.dump((pers,appls),open("1-"+fname,'w'))
                
                if exists("2-"+fname):
                    pers,appls = cPickle.load(open("2-"+fname,'r'))
                else:
                    i=0
                    j=len(appls)    
                    for k,v in appls.iteritems():
                        if i % 10000 == 0: print i,'on',j
                            i+=1
                            first_date = date.today()
                            
                            for p in v['prior']:
                                c.execute('''SELECT appln_filing_date FROM patstat201204.TLS201_APPLN WHERE appln_id=%s ''' % p) 
                                dd=c.fetchone()
                                if dd is None:
                                    continue
                                dd=dd[0]
                                if dd<first_date:
                                    first_date = dd
                                    
                                    if v['date'] is None: 
                                        c.execute('''SELECT appln_filing_date FROM patstat201204.TLS201_APPLN WHERE appln_id=%s ''' % k) 
                                        dd=c.fetchone()
                                        if dd is not None:
                                            v['date']=(dd[0],)
                                        else:
                                            v['date']=(date.today(),)
                                            if v['date'][0]<first_date:
                                                first_date = v['date'][0]
                                                
                                                appls[k]['pdate']=first_date if v['prior'] else v['date'][0]
                                                #print k,appls[k]
        
    cPickle.dump((pers,appls),open("2-"+fname,'w'))            



    #pprint(appls)

db_drop_table(c,'ITA_PERS_APPLN')
sql="""CREATE TABLE ITA_PERS_APPLN ( person_id INT(9) NOT NULL, appln_id INT(9) NOT NULL, prior_date DATE ,year INT(9))""" 
c.execute(sql)
table = []
for id,applz in pers.items():
    for pid in applz:
        pd = None
        y = None
        if pid[0] in appls:
            Z=appls[pid[0]]
            pd = Z['pdate']
            y = int(str(pd)[0:4])
        tv = (id[0],pid[0],pd,y) 
        sql="""INSERT INTO ITA_PERS_APPLN(person_id,appln_id,prior_date,year) VALUES (%s,%s,%s,%s)""" 
        c.execute(sql,tv)
db.commit()

sql="""ALTER TABLE `ITA_PERS_APPLN` ADD INDEX person_id (`person_id`)"""
c.execute(sql)

sql="""ALTER TABLE `ITA_PERS_APPLN` ADD INDEX appln_id (`appln_id`)"""
c.execute(sql)

db.close()

print "END"
