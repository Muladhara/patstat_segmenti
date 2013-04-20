# -*- coding: utf-8 -*-

# ----
from collections import defaultdict
from itertools import ifilter
from pprint import pprint
import MySQLdb
import re
import sys
import copy
import os
from Levenshtein import *

sys.path.append(os.getcwd())
from patstat.cleaning import *

pwd=os.getenv('PWD')
db=MySQLdb.connect(user="root",passwd=pwd,db="workaddr",use_unicode=True,unix_socket='/var/mysql/mysql.sock')
c=db.cursor()


## ################################################## Carica i comuni ISTAT

class Comune(object):
    def __init__(self,CAP,COMUNE,PROVINCIA,REGIONE):
        self.CAP = CAP
        self.COMUNE = COMUNE
        self.PROVINCIA = PROVINCIA
        self.REGIONE = REGIONE

estratti = set()        
c.execute("""SELECT CAP,COMUNE,PROVINCIA,REGIONE FROM comuni  """)
cn,cc,cb = {},{},{}
for CAP,COMUNE,PROVINCIA,REGIONE in c.fetchall():    
    COMUNE = COMUNE.replace("'",' ').replace("  ",' ').strip()
    if '/' in COMUNE:
        cx = COMUNE.split('/')
        COMUNE = cx[0]

    # ac - oggetto che contiene tutte le informazioni
    #      del comune
    ac = Comune(CAP,COMUNE,PROVINCIA,REGIONE)
    cn[COMUNE] = ac
    cc[CAP] = ac
    bow = COMUNE.split()
    for cx in bow:
        if cx in cb.keys():
            del cb[cx]
        else:
            cb[cx]=ac    

print "# Comuni nel db ISTAT =",len(cn)," #bow=",len(cb.keys())

c.execute("""SELECT CAP,COMUNE,PROVINCIA,REGIONE FROM comuni WHERE CAPOLUOGO=1  """)
pn = {}
for CAP,COMUNE,PROVINCIA,REGIONE in c.fetchall():
    COMUNE = COMUNE[1:-1] if COMUNE[0]=='\'' else COMUNE
    ac = Comune(CAP,COMUNE,PROVINCIA,REGIONE)
    pn[COMUNE] = ac
print "# Provincie nel db ISTAT =",len(pn)


def get_cap(name,dbresult1,cc,estratti=set()):
    dbresults = copy.copy(dbresults1)
    dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
    if allTables:
        db_save_table(c,dict([ (k,(v,)) for k,v in dbresults]),name+'_inA',save=name+"_inAsav")
    dbresults = transform_2nd_in_list(dbresults,( 
            (re.compile(r'[^0-9]+'),' '), 
            (re.compile(r'\b[0-9]{1,4}\b'),''), 
            (re.compile(r'\b[0-9]{6,}\b'),''), 
            (re.compile(r' '),''), 
            ))
    dbresults = [ n for n in ifilter(lambda x: len(x[1])==5,dbresults)]
    if allTables:
        db_save_table(c,dict([ (k,(v,)) for k,v in dbresults]),name+'_in',save=name+"_insav")
    print "# Record letti come ",name,":",len(dbresults)
    city = dict([ (k,(cc[v],))
                  for k,v in dbresults if v in cc.keys()])
    cityn = dict([ (k,(v[0].PROVINCIA,))
                   for k,v in city.items()])
    print "# ",name," riconosciuti:",len(cityn)
    db_save_table(c,cityn,name,save=name+"_sav")
    if allTables:
        residui = [ r for r in dbresults if r[0] not in cityn.keys() ]
        db_save_table(c,residuals,name+'_res',save=name+"_rsav")
    return set(cityn)    

# select a.id1,a.val1,b.val3 from base b join T_1_city_sav a using(id1) 

estratti = set()

## ---------------------------------------- FASE 1: ELEMENTI VUOTI
## Record con address e city vuota
## 21046 record
print "---------------------------------------- FASE 1: ELEMENTI VUOTI"
c.execute(""" SELECT id1 FROM base WHERE val3 = '' and val4 = ''  """)
dbresults1 = c.fetchall()
vuoti = [ k[0] for k in dbresults1]
db_save_table(c,vuoti,'__empty')
estratti |= set(vuoti)

## ----------------------------------------  FASE 2: RICERCA CAP in ADDRESS (val4) se val3 vuoto
## Prende i CAP dal campo 4 quando il campo 3 è vuoto
## 1256 record
print "----------------------------------------  FASE 2: RICERCA CAP in ADDRESS (val4)"
c.execute(""" SELECT id1,val4 FROM base WHERE val3 = '' and val4 != ''  """)
dbresults1 = c.fetchall()
print "# db rows",len(dbresults1)
estratti |= get_cap('__CAP_on_val4',dbresults1,cc,estratti)

## ----------------------------------------  FASE 3: RICERCA CAP in CITY (val3)
## Prende i CAP dal campo 3
## 102298 record
print "----------------------------------------  FASE 3: RICERCA CAP in CITY (val3)"
c.execute(""" SELECT id1,val3 FROM base WHERE val3 != '' """)
dbresults1 = c.fetchall()
print "# db rows",len(dbresults1)
estratti |= get_cap('__CAP_on_val3',dbresults1,cc,estratti)

## -------------------------------------------------- FASE 4: CITTA (E PROVINCIA) CONTENUTE in CITY
## Prende la città e la prov nel campo city (dopo trattamento)
## 0 record
addr_treatment=( 
        (re.compile(r'\([^)]\)'),''), 
        (re.compile(r'\bI-'),''), 
        (re.compile(r'(I-)?[0-9]+'),''), 
        (re.compile(r'[^A-Z\s]+'),' '), 
        (re.compile(r'\s+\b[A-Z][A-Z]$'),''), 
        (re.compile(r'\s+'),' '), 
        (re.compile(r'\bMILAN\b'),'MILANO'), 
        (re.compile(r'\bMAILAND\b'),'MILANO'), 
        (re.compile(r'\bGENOA\b'),'GENOVA'), 
        (re.compile(r'\bGENUA\b'),'GENOVA'), 
        (re.compile(r'\bROME\b'),'ROMA'), 
        (re.compile(r'\bBOLOGNE\b'),'BOLOGNA'), 
        (re.compile(r'\bROM\b'),'ROMA'), 
        (re.compile(r'\bTURIN\b'),'TORINO'), 
        (re.compile(r'\bFLORENCE\b'),'FIRENZE'), 
        (re.compile(r'\bFLORENZ\b'),'FIRENZE'), 
        (re.compile(r'\bVERONE\b'),'VERONA'), 
        (re.compile(r'\bPADUA\b'),'PADOVA'), 
        (re.compile(r'\bVENICE\b'),'VENEZIA'), 
        (re.compile(r'\bMESTRE\b'),'VENEZIA'), 
        (re.compile(r'\bNAPLES\b'),'NAPOLI'), 
        (re.compile(r'\bFORLI?\b'),u'FORLÌ'), 
        (re.compile(r'\bREGGIOs+EMILIA'),u'REGGIO NELL EMILIA'), 
        (re.compile(r'\bREGGIOs+CALABRIA'),u'REGGIO DI CALABRIA'), 
        (re.compile(r'\s+$'),''), 
        (re.compile(r'^\s+'),''), 
        )

db_mk_support_tables(c)
c.execute("""SELECT id1,val3 FROM base WHERE val3 != '' """)
dbresults1 = c.fetchall()
print "# db rows",len(dbresults1)
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)
d,wtr,wall = loadall(dbresults,estratti,bow=False)


def search_city(wtr,cn):
    citya = dict( [ (k,v) for k,v in wtr.items() if k in cn.keys() ] )
    city = dict()
    for wrd,ids in citya.items():
        for id in ids:
            city[id]=(cn[wrd],)

    cityn = dict([ (k,(v[0].PROVINCIA,))
                   for k,v in city.items()])
    return cityn

cityn = search_city(wtr,cn)

db_save_table(c,cityn,None,save="__city")
estratti |= set(cityn)

## -------------------------------------------------- FASE 5: CITTA (E PROVINCIA) CONTENUTE NELLA BOW
## Cerca il capoluogo di provincia o la sigla della provincia nella bag of words
## 
print " ---------------------------------------- Fase 5"
db_mk_support_tables(c)
c.execute("""SELECT id1,val3 FROM base""")
dbresults1 = c.fetchall()
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)

d,wtr,wall = loadall(dbresults,set(),bow=True)

# estrazione nomi o sigle di province
s=list()
for k in pn.keys():       # nome  di provincia
    k1 = pn[k].PROVINCIA  # sigla di provincia
    if k in wtr: 
        for s1 in wtr[k]: 
            s.append((k1,s1))
    if k1 in wtr:
        for s1 in wtr[k1]:
            s.append((k1,s1))

# Search in bow 
for k in cb.keys():
    if k in wtr:
        for s1 in wtr[k]:
            s.append((cb[k].PROVINCIA,s1))
            

recognized=dict([ (v,(k,)) for k,v in s ])
estratti |= set(recognized.keys())

db_save_table(c,recognized,None,save="__subcity")


### ----------------------------------------

## ----------------------------------------  FASE 6: RICERCA CAP in CITY (val4) indipendentemente da val3
## Prende i CAP dal campo 4 quando il campo 3 è vuoto
## 20 record
print " ---------------------------------------- Fase 6"
c.execute(""" SELECT id1,val4 FROM base  """)
dbresults1 = c.fetchall()
print "# db rows",len(dbresults1)
estratti |= get_cap('__CAP_on_val4_II',dbresults1,cc,estratti)

## ----------------------------------------  FASE 7: RICERCA COMUNE in CITY (val4)
## 
## 6 record
dbresults = copy.copy(dbresults1)
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)
d,wtr,wall = loadall(dbresults,estratti,bow=False)

citya = dict( [ (k,v) for k,v in wtr.items() if k in cn.keys() ] )
city = dict()
for wrd,ids in citya.items():
    for id in ids:
        city[id]=(cn[wrd],)

cityn = dict([ (k,(v[0].PROVINCIA,))
                for k,v in city.items()])
print len(cityn)
db_save_table(c,cityn,None,save="__city_II")
estratti |= set(cityn)

## ----------------------------------------  FASE 8: RICERCA PROVINCIA in bow di ADDRESS (val4)
## Prende la provincia nella bow di address
## 861 record
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)

d,wtr,wall = loadall(dbresults,set(),bow=True)

s=list()
for k in pn.keys():
    k1 = pn[k].PROVINCIA
    if k in wtr:
        for s1 in wtr[k]:
            s.append((k1,s1))
    if k1 in wtr:
        for s1 in wtr[k1]:
            s.append((k1,s1))

# Search in bow
for k in cb.keys():
    if k in wtr:
        for s1 in wtr[k]:
            s.append((cb[k].PROVINCIA,s1))
            
#recognized=dict([ (v,(k+"|"+' '.join(d[v]),)) for k,v in s ])
recognized=dict([ (v,(k,)) for k,v in s ])
estratti |= set(recognized.keys())

db_save_table(c,recognized,None,save="__subcity_II")

## ----------------------------------------  FASE 9: RICERCA PROVINCIA in (...) di ADDRESS (val3)
## 2 record
print " ---------------------------------------- Fase 6"
c.execute(""" SELECT id1,val3 FROM base where val3 like "%(%)%" """)
dbresults1 = c.fetchall()
print "# db rows",len(dbresults1)
dbresults = copy.copy(dbresults1)
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,(
                (re.compile(r'^.*\('),''),
                (re.compile(r'\).*$'),''),
                ))
# db_save_table(c,dict([ (k,(v,)) for k,v in dbresults]),'__residui_saveX',save="__residuiX")          
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)
d,wtr,wall = loadall(dbresults,estratti,bow=False)

citya = dict( [ (k,v) for k,v in wtr.items() if k in cn.keys() ] )
city = dict()
for wrd,ids in citya.items():
    for id in ids:
        city[id]=(cn[wrd],)

cityn = dict([ (k,(v[0].PROVINCIA,))
                for k,v in city.items()])
print len(cityn)
db_save_table(c,cityn,None,save="__city_III")
estratti |= set(cityn)

## ----------------------------------------  FASE 10: RICERCA PROVINCIA in (...) di CITY (val4)
## 0 record
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
print len(dbresults)

d,wtr,wall = loadall(dbresults,set(),bow=True)

s=list()
for k in pn.keys():
    k1 = pn[k].PROVINCIA
    if k in wtr:
        for s1 in wtr[k]:
            s.append((k1,s1))
    if k1 in wtr:
        for s1 in wtr[k1]:
            s.append((k1,s1))
            

recognized=dict([ (v,(k,)) for k,v in s ])
estratti |= set(recognized.keys())

db_save_table(c,recognized,None,save="__subcity_III")


## ------------------------------------------------------------

c.execute('drop table if exists _all')

c.execute('create table _all as select * from __CAP_on_val3 union select * from __CAP_on_val4 union select * from __city union select * from __subcity union select * from __city_II union select * from __subcity_II union select * from __city_III union select * from __subcity_III ' )

c.execute('drop table if exists _all_sav')
c.execute('create table _all_sav as select * from __CAP_on_val4_sav union select * from __CAP_on_val3_sav union select * from __city union select * from __subcity union select * from __city_II union select * from __subcity_II union select * from __city_III union select * from __subcity_III ' )


c.execute('drop table if exists otherdb.location')
c.execute('create table otherdb.location as select id1 as person_id,val1 location from _all_sav')

c.execute('alter table otherdb.location add primary key person_id (person_id)')
c.execute('alter table otherdb.location modify column location char(2)' )

db_mk_support_tables(c)
c.execute("""SELECT id1,val3 FROM base""")
dbresults1 = c.fetchall()
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)
dbresults = [ (k,v) for k,v in dbresults if k not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_treatment)

db_save_table(c,dict([ (k,(v,)) for k,v in dbresults]),'__residui_save',save="__residui")

d,wtr,wall = loadall(dbresults,estratti,bow=True)

db_mk_support_tables(c)
name = 'residuals'
print "Saving ZZ_wtr"
for k,v in wtr.iteritems():
    res = [ (w,k) for w in v ]
    c.executemany("""INSERT INTO support4(id1,val1) VALUES (%s,%s)""", res)
db_rename_table(c,'support4','T_%s_ZZ_wtr' % name )

c.execute('drop view if exists residual_words')
c.execute('create view residual_words as select val1,count(id1) from T_%s_ZZ_wtr group by val1 order by count(ID1) desc limit 100;' % name)

