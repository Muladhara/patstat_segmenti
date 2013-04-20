# -*- coding: utf-8 -*-

# ----
from collections import defaultdict
from itertools import ifilter
import MySQLdb
import re
import sys
import copy
import os

sys.path.append(os.getcwd())
from patstat.cleaning import *

doUpdate=False
doZZsave=False
allTables=False

print "BEGIN program"

deletions = []

## ######################################## LISTE

patterns_enti = (
    re.compile(r"\bALTA\s+RICERCA\b",flags=re.I),
    re.compile(r"\bAGENZIA\b",flags=re.I),
    re.compile(r"\bANTON\s+DOHRN\b",flags=re.I),
    re.compile(r"\bCONSIGLIO\s+NAZIONALE\b",flags=re.I),
    re.compile(r"\bENTE\b",flags=re.I),
    re.compile(r"\bFONDAZIONE\b",flags=re.I),
    re.compile(r"\bINTERUNIVERSITARIO\b",flags=re.I),
    re.compile(r"\bISTITUTO\s+NAZIONALE\b",flags=re.I),
    re.compile(r"\bISTITUTO\s+SUPERIORE\b",flags=re.I),
    re.compile(r"\bIST\s+NAZIONALE\b",flags=re.I),
    re.compile(r"\bMAX\s+PLANCK\s+INSTITUT\b",flags=re.I),
    re.compile(r"\bNANOFABER\b",flags=re.I),
    re.compile(r"\bVULCANOLOGIA\b",flags=re.I),
#    re.compile(r"\bA[. ]?S[. ]?I[. ]?\b",flags=re.I),
#    re.compile(r"\bENRICO\s+FERMI\b",flags=re.I),   (via enrico fermi)
#    re.compile(r"\bE[. ]?N[. ]?E[. ]?A[. ]?\b",flags=re.I),
#    re.compile(r"\bA[. ]?S[. ]?I[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?E[. ]?I[. ]?N[. ]?G[. ]?E[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?I[. ]?R[. ]?C[. ]?C[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?I[. ]?R[. ]?M[. ]?M[. ]?P[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?I[. ]?T[. ]?I[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?N[. ]?I[. ]?T[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?N[. ]?R[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?N[. ]?R[. ]?\b",flags=re.I),
    re.compile(r"\bC[. ]?R[. ]?A[. ]?\b",flags=re.I),
    re.compile(r"\bE[. ]?I[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bE[. ]?N[. ]?S[. ]?E[. ]?\b",flags=re.I),
    re.compile(r"\bE[. ]?S[. ]?A[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?A[. ]?S[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?A[. ]?F[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?A[. ]?F[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?B[. ]?B[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?D[. ]?A[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?D[. ]?A[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?E[. ]?A[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?F[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?F[. ]?N[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?F[. ]?N[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?G[. ]?V[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?G[. ]?V[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?R[. ]?A[. ]?N[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?R[. ]?I[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?R[. ]?I[. ]?M[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?S[. ]?E[. ]?A[. ]?N[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?V[. ]?A[. ]?L[. ]?S[. ]?I[. ]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?P[. ]?E[. ]?S[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?I[. ]?M[. ]?E[. ]?C[. ]?H[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?F[. ]?O[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?M[. ]?E[. ]?A[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?T[. ]?A[. ]?T[.]?\b",flags=re.I),
    re.compile(r"\bM[. ]?U[. ]?R[. ]?S[. ]?T[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?A[. ]?E[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?S[. ]?S[.]?\b",flags=re.I),
    re.compile(r"\bO[. ]?G[. ]?S[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?A[. ]?P[. ]?A[. ]?\b",flags=re.I),

#    re.compile(r"\bI[. ]?S[. ]?P[. ]?R[. ]?A[. ]?\b",flags=re.I),   ISPRA (varese),
    )
patterns_aziende=(
    re.compile(r"\bS[. ]?C[. ]?A[. ]?R[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?A[. ]?R[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?C[. ]?P[. ]?A[.]?\b",flags=re.I),
    re.compile(r"\bA[. ]?C[. ]?E[. ]?A[.]?\b",flags=re.I),
    re.compile(r"\bF[. ]?I[. ]?A[. ]?T[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?R[. ]?B[. ]?M[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?C[. ]?R[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bC[. ]?O[. ]?O[. ]?P[.]?\b",flags=re.I),
    re.compile(r"\bG[. ]?M[. ]?B[. ]?H[.]?\b",flags=re.I),
    re.compile(r"\bE[. ]?N[. ]?E[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bL[. ]?L[. ]?C[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?R[. ]?L[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?N[. ]?C[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?P[. ]?A[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?A[. ]?S[.]?\b",flags=re.I),
    re.compile(r"\bL[. ]?T[. ]?D[.]?\b",flags=re.I),
    re.compile(r"\bC[. ]?R[. ]?F[.]?\b",flags=re.I),
    re.compile(r"\bI[. ]?N[. ]?C[.]?\b",flags=re.I),
    re.compile(r"\bL[. ]?T[. ]?D[.]?\b",flags=re.I),
    re.compile(r"\bE[. ]?N[. ]?I[.]?\b",flags=re.I),
    re.compile(r"\bN[. ]?M[. ]?N[.]?\b",flags=re.I),
    re.compile(r"\bP[. ]?L[. ]?C[.]?\b",flags=re.I),
    re.compile(r"\bS[. ]?C[. ]?H[.]?\b",flags=re.I),
    re.compile(r"\bA[. ]?G[.]?\b",flags=re.I),
    re.compile(r"\bC[. ]?O[.]?\b",flags=re.I),
)

#
addr_repl = (
    (re.compile(r"\bLOCALIT.?\b",flags=re.I), 'LOCALITA'),    
    (re.compile(r"(\b(VIA|PIAZZA|VIALE|V\.\s*LE|C\.SO|P\.\s*LE|LARGO|PIAZZALE|RIONE|PIAZZA|VICOLO|CORSO|STRADA|S\.S\.|ZONA\sINDUSTRIALE|LOCALIT(A|À))\b.*)$",flags=re.I), ''), 
    (re.compile(r"^\s*[0-9/]+[,\s]*$",flags=re.I), ''), 
    (re.compile(r"^NO?\.?\s*[0-9/]+[,\s]*$",flags=re.I), ''), 
    (re.compile(r"'",flags=re.I), ' '), 
    )
#
base_repl = (
    (re.compile(r"\bSIGMA[- ]?TAU\b",flags=re.I), 'SIGMATAU'),    
    (re.compile(r"\bDEGLI\s+STUDI\b",flags=re.I), ' UNIVERSITA '),    
    (re.compile(r"UNIVERSIT.",flags=re.I), ' UNIVERSITA '),
    (re.compile(r"\bSOCIET.",flags=re.I), ' SOCIETA '),    
    (re.compile(r"\bC(ONS(IGLIO)?)?\.\s+NAZ(IONALE)?\.?\s+.+RICERCHE",flags=re.I), 'CNR'),    
    (re.compile(r"\bSOCIET.'?\s*(CONSORTILE\s*)?PER\s+AZIONI",flags=re.I), 'SPA'),    
    (re.compile(r"\bSOCIET.'?\s+A\s+RESP(ONSABILIT.'?)\s+LIMITATA",flags=re.I), 'SRL'),    
    (re.compile(r"\bUNI\.",flags=re.I), 'UNIVERSITA '),    
    (re.compile(r"[.\s,]+",flags=re.I), ' '),    
    (re.compile(r"\bUNIVER(SIT.?)\b",flags=re.I), 'UNIVERSITA'),    
    (re.compile(r"\bS[. ]*?C[. ]*?A[. ]*?R[. ]*?L[.]*?\b",flags=re.I),'SCARL'),
    (re.compile(r"\bC[. ]*?O[. ]*?O[. ]*?P[.]*?\b",flags=re.I),'COOP'),
    (re.compile(r"\bE[. ]*?N[. ]*?E[. ]*?L[.]*?\b",flags=re.I),'ENEL'),
    (re.compile(r"\bA[. ]*?C[. ]*?E[. ]*?A[.]*?\b",flags=re.I),'ACEA'),
    (re.compile(r"\bF[. ]*?I[. ]*?A[. ]*?T[.]*?\b",flags=re.I),'FIAT'),
    (re.compile(r"\bG[. ]*?M[. ]*?B[. ]*?H[.]*?\b",flags=re.I),'GMBH'),
    (re.compile(r"\bI[. ]*?R[. ]*?B[. ]*?M[.]*?\b",flags=re.I),'IRBM'),
    (re.compile(r"\bS[. ]*?A[. ]*?P[. ]*?A[.]*?\b",flags=re.I),'SAPA'),
    (re.compile(r"\bS[. ]*?A[. ]*?R[. ]*?L[.]*?\b",flags=re.I),'SARL'),
    (re.compile(r"\bS[. ]*?C[. ]*?R[. ]*?L[.]*?\b",flags=re.I),'SCRL'),
    (re.compile(r"\bS[. ]*?C[. ]*?P[. ]*?A[.]*?\b",flags=re.I),'SCPA'),
    (re.compile(r"\bC[. ]*?R[. ]*?F[.]*?\b",flags=re.I),'CRF'),
    (re.compile(r"\bE[. ]*?N[. ]*?I[.]*?\b",flags=re.I),'ENI'),
    (re.compile(r"\bI[. ]*?N[. ]*?C[.]*?\b",flags=re.I),'INC'),
    (re.compile(r"\bL[. ]*?L[. ]*?C[.]*?\b",flags=re.I),'LLC'),
    (re.compile(r"\bL[. ]*?T[. ]*?D[.]*?\b",flags=re.I),'LTD'),
    (re.compile(r"\bL[. ]*?T[. ]*?D[.]*?\b",flags=re.I),'LTD'),
    (re.compile(r"\bN[. ]*?M[. ]*?N[.]*?\b",flags=re.I),'NMN'),
    (re.compile(r"\bP[. ]*?L[. ]*?C[.]*?\b",flags=re.I),'PLC'),
    (re.compile(r"\bS[. ]*?A[. ]*?S[.]*?\b",flags=re.I),'SAS'),
    (re.compile(r"\bS[. ]*?C[. ]*?H[.]*?\b",flags=re.I),'SCH'),
    (re.compile(r"\bS[. ]*?N[. ]*?C[.]*?\b",flags=re.I),'SNC'),
    (re.compile(r"\bS[. ]*?P[. ]*?A[.]*?\b",flags=re.I),'SPA'),
    (re.compile(r"\bS[. ]*?R[. ]*?L[.]*?\b",flags=re.I),'SRL'),
    (re.compile(r"\bS[. ]*?R[. ]*?1[.]*?\b",flags=re.I),'SRL'),
    (re.compile(r"\bA[. ]*?G[.]*?\b",flags=re.I),'AG'),
    (re.compile(r"\bC[. ]*?O[.]*?\b",flags=re.I),'CO'),
    (re.compile(r"[^\&\/A-ZÀÈÉÌÒÙ0-9 ]",flags=re.I), ' '), 
    (re.compile(r"[^\&\/A-ZÀÈÉÌÒÙ0-9 ]",flags=re.I), ' '), 
    (re.compile(r"[.\s,]+",flags=re.I), ' '),    
    )
firm_stop_list = (
    'ACEA',            # 0
    'AG',
    'C/O',
    'CHEM',
    'CO',
    'COOPERATIVA',
    'COMMUNITY',
    'COMPANY',
    'COOP',
    'CORP',
    'CORPORATION',
    'CRF',             # 10
    'ENEL',
    'ENI',
    'EUROPE',
    'EUROPEAN',
    'FIAT',
    'GMBH',
    'GROUP',
    'HOLDING',
    'HOLDINGS',
    'IMPRESA',
    'INCORPORATED',    # 20
    'INTERNATIONAL',
    'IRBM',
    'LIMITED',
    'LLC',
    'LTD',
    'LTD',
    'MEDICAL',
    'NMN',
    'OFFICINA',
    'PHARMA',
    'PLC',             # 30
    'RESPONSABILITA',
    'RESPONSABILITÀ',
    'SAPA',
    'SARL',
    'SAS',
    'SCARL',
    'SCRL',
    'SCH',
    'SCPA',
    'SERVICES',
    'SNC',
    'SOCIETA',
    'SOCIETÀ',
    'SRL',            # 40           
    'SPA',
    'SYSTEMS',
    'TECHNOLOGY',
    'TECHNOLOGIES',
    'GLAXO',
    'GLAXOSMITHKLINE',
    'MOTOR',
    'SISTEMI',
    'FALEGNAMERIA',
    'SIGMATAU',
    'RAI',
    'OSSERVATORIO',
    'BENCKISER'
)
uni_stop_list = (
    u'UNIV',
    u'UNIVERSITA',
    u'UNIVERSITÀ',
    u'UNIVERSITY',
#    'LA SAPIENZA',
#    'TOR VERGATA',
    u'FACOLTA',
    u'FACOLTÀ',
    u'DIPARTIMENTO',
    u'DEPARTMENT',
    u'ISTITUTO',
    u'SCUOLA',
    u'POLITECNICO'
)

debug = False


uni = None
firms = None
d, wtr, wall = None, None, None
u_trace, f_trace = None, None

def step_01(name,dbresults,estratti):
    global uni, firms, d, wtr,u_trace,f_trace
    
    d,wtr,wall = loadall(dbresults,estratti)
    print "Ingresso"
    removed,w=clean_d(d,wtr)
    print_coll(d,wtr,wall)
    if allTables:
        db_save_table(c,removed,'T_%s__input_removed' % name )
        db_save_table(c,d,'T_%s_input' % name )

    ## FAST .2 - UNI
    uni,u_trace = select_from_stop_list(name,uni_stop_list,d,wtr)
    remove_from_sets(uni,d,wtr)
    delete_from_stop_list(uni_stop_list,d,wtr)
    print "Università ",len(uni),"E&U",estratti & set(uni)
    removed,w=clean_d(d,wtr)
    print_coll(d,wtr,wall)
    if allTables:
        db_save_table(c,removed,'T_%s__uni_removed' % name )
    db_save_table(c,uni,'T_%s__uni' % name )
    db_updatemany(c,'T_%s__uni'%name,'val6',u_trace)
    estratti |= set(uni)
    
    dbresults = [ x for x in dbresults if x[0] not in estratti]    
    d,wtr,wall = loadall(dbresults,estratti)
    print "Ingresso"
    removed,w=clean_d(d,wtr)
    print_coll(d,wtr,wall)

    D=len(d)
    firms,f_trace = select_from_stop_list(name,firm_stop_list,d,wtr)
    remove_from_sets(firms,d,wtr)
    delete_from_stop_list(firm_stop_list,d,wtr)
    removed,w=clean_d(d,wtr)
    print_coll(d,wtr,wall)
    if allTables:
        dbs_ave_table(c,removed,'T_%s__firms_removed' % name )
    print "Aziende ",len(firms),"=",D-len(d),"E&F",estratti & set(firms),"UNI&F",set(uni) & set(firms)
    db_save_table(c,firms,'T_%s__firms' % name )
    db_updatemany(c,'T_%s__firms'%name,'val6',f_trace)
    estratti |= set(firms)
    
    if allTables and doZZsaves:
        print "Saving ZZ_d"
        db_mk_support_tables(c)
        res = [ (k,' '.join(v)) for k,v in d.items() ]
        c.executemany("""INSERT INTO support3(id1,val1) VALUES (%s,%s)""", res)
        db_rename_table(c,'support3','T_%s_ZZ_d' % name )

        print "Saving ZZ_wtr"
        for k,v in wtr.iteritems():
            res = [ (w,k) for w in v ]
            c.executemany("""INSERT INTO support4(id1,val1) VALUES (%s,%s)""", res)
        db_rename_table(c,'support4','T_%s_ZZ_wtr' % name )

    return d,wtr,wall,estratti


def get_enti(name,dbresults):

    # Regexp matching 
    dbresults = transform_2nd_in_list(dbresults,addr_repl)
    dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]

    # Base Regexp matching 
    dbresults = transform_2nd_in_list(dbresults,base_repl)

    if allTables:
        db_save_table(c,mkdict(dbresults),'T_%s_tC'%name,save='T_%s_tD'%name)
    
    #  
    enti_potenziali,dbresults = grep_from_list(dbresults,patterns_enti,take2nd)
    esclusi,enti = grep_from_list(enti_potenziali,patterns_aziende, take2nd)
    enti = [ x[0] for x in enti ]
    return enti

take2nd = lambda x: x[1]
mkdict = lambda dbresults: dict([ (n,(k,)) for n,k in dbresults])

pwd=os.getenv('PWD')
db=MySQLdb.connect(user="root",passwd=pwd,db="work",use_unicode=True,unix_socket='/var/mysql/mysql.sock')
c=db.cursor()

## ----------------------------------------  FASE 1: RICERCA ENTI, UNIVERSITA E AZIENDE SU ADDRESS
estratti = set()
c.execute("""SELECT id1,val3 FROM base""")
dbresults1 = c.fetchall()
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)

# FASE 1.1 - ENTI
enti = get_enti('0',dbresults)
db_save_table(c,enti,'T_0__enti')
e_trace=mk_work_trace(enti,val6='0.0')
db_updatemany(c,'T_0__enti','val6',e_trace)
estratti |= set(enti)

print "Ho fin qui estratto",len(estratti),"elementi"

# FASE 1.2-3 - UNI FIRMS
dbresults = [ x for x in dbresults1 if x[0] not in estratti]
dbresults = transform_2nd_in_list(dbresults,addr_repl)
dbresults = [ n for n in ifilter(lambda x: len(x[1])>0,dbresults)]
dbresults = transform_2nd_in_list(dbresults,base_repl)

print "Step 1 on",len(dbresults),'elements'
d,wtr,wall,estratti = step_01('0',dbresults,estratti)

print "Ho fin qui estratto",len(estratti),"elementi"

## ---------------------------------------- FASE  2:  RICERCA, UNIVERSITA ENTI E AZIENDE SU LAST_NAME
d,wtr,wall = None,None,None
c.execute("""SELECT id1,val1 FROM base""")
dbresults1 = c.fetchall()
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)

### FASE 2.1 ENTI
enti = get_enti('1',dbresults)
db_save_table(c,enti,'T_1__enti')
e_trace=mk_work_trace(enti,val6='1.0')
db_updatemany(c,'T_1__enti','val6',e_trace)
estratti |= set(enti)

### FASI 2.2-3 UNI FIRMS
dbresults = [ x for x in dbresults1 if x[0] not in enti]
dbresults = transform_2nd_in_list(dbresults,base_repl)
print "Step 1 on",len(dbresults),'elements'
d,wtr,wall,estratti = step_01('1',dbresults,estratti) 
print "end Step 1"

### FASE 2.4 FIRMS WITH '&'
firms_with_ampersand = set(wall['&'])
remove_from_sets(firms_with_ampersand,d,wtr)
db_save_table(c,firms_with_ampersand,'T_1__firms_w_amp')
print "Aziende & ",len(firms_with_ampersand)
removed,w=clean_d(d,wtr)
if allTables:
    db_save_table(c,removed,'T_1__firms_w_app_removed' )
print_coll(d,wtr,wall)
estratti |= set(firms_with_ampersand)

if allTables:
    db_save_table(c,mkdict(deletions),'T_delete',save='T_delete_D')

## ---------------------------------------- FASE  3:  PERSONE
c.execute("""SELECT name FROM names UNION SELECT name FROM surnames""")
names = set(' '.join([ x[0] for x in c.fetchall()]).split(' '))
print "names in=",len(names)

persons = dict([ (k,v) for k,v in d.items() if reduce(lambda x,y: x and (len(y)<2 or (y in names)),v)])
print "Persons=",len(persons)
residuals = dict([ (k,v) for k,v in d.items() if not reduce(lambda x,y: x and (len(y)<2 or (y in names)),v)])

db_save_table(c,persons,'T_1__person')
db_save_table(c,residuals,'residuals')
cleanup(c)
estratti |= set(persons)

## ---------------------------------------- FASE  4:  EURISTICHE
# Euristiche

d,wtr,wall = None,None,None
c.execute("""SELECT id1,val1 FROM residuals""")
dbresults1 = c.fetchall()
dbresults1 = map(lambda x: (x[0],x[1].upper()),dbresults1)
dbresults = copy.copy(dbresults1)
dbresults = transform_2nd_in_list(dbresults,base_repl)
print "Step 1 on",len(dbresults),'elements'
d,wtr,wall = loadall(dbresults,[]) 

esclusi=set()
e0_ids = (
    (8831744,'firm'),
    (6922220,'firm')
)
e1_enti_sw = (
    u'CONSIGLIO',
)
stopped = dict([ (k,v) for k,v in d.items() if k not in estratti and reduce(lambda x,y: x or (y in e1_enti_sw),v,False)])
db_save_table(c,stopped,'T_1__euristica_1_enti')
e_trace=mk_work_trace(enti,val6='e1.0')
db_updatemany(c,'T_0__eurisitica_1_enti','val6',e_trace)
estratti |= set(stopped.keys())
e1_uni_sw = (
    u'DEPT',
    u'DIP',
    u'UNI',
    u'UNV',    
)
stopped = dict([ (k,v) for k,v in d.items() if k not in estratti and reduce(lambda x,y: x or (y in e1_uni_sw),v,False)])
db_save_table(c,stopped,'T_1__euristica_1_uni')
estratti |= set(stopped.keys())
dont_know = (
    u'LAB',
    u'TECNOPOLIS',
    u'INTERNAZIONALE',
    u'ITALIANO',
    u'STUDIO',
    u'STAZIONE',
    u'RICERCA',
    u'RICERCHE',
    u'CANCRO',
    u'CANCER',
    u'ISPEL',
    u'LATTIERO',
    u'POLIGRAFICO',
    u'CENTRO',    
    u'CENTRE',    
    u'IST',    
    u'INST',    
    u'INSTITUTE',    
    u'INSTITUTO',    
    u'INSTITUTI',    
    u'ISTITUTI',
    u'LABORATORIO',    
    u'DEVELOPMENT',    
    u'SCIENTIFIC',    
    u'RESEARCH',    
    u'DEVELOPMENT',    
    u'MATERIA',    
    u'CONSORZIO',    
    u'CONZORZIO',    
    u'HOSPITAL',    
    u'OSPEDALE',    
    u'SPERIMENTALE',    
    u'CORECOM',    
    u'COUNCIL',    
    u'LABS',    
    u'POLICLINICO',
    u'TECNOLOGICO',
    u'FARMACO',
    u'DIFFUSION',
    u'OSPITALIERI',
    u'FISIOTERAPICI',
)
stopped = dict([ (k,v) for k,v in d.items() if k not in estratti and reduce(lambda x,y: x or (y in dont_know),v,False)])
db_save_table(c,stopped,'T_1__euristica_1_dont_know')
estratti |= set(stopped.keys())
# regola "azienda di persona"
stop_words = (
    u'OFFICINE',
    u'DITTA',
    u'FRATELLI',
    u'SYSTEM',
    u'FARMITALIA',
    u'PRODUZIONE',
    u'SOC',
    u'OLIVETTI',
    u'CREATE',
    u'MACHINERY',
    u'CASALINGHI',
    u'ARCHITETTURA',
    u'METALLURGICA',
    u'ASSOCIAZIONE',
    u'OMNITEL',
    u'ROMA',
    u'FERRARA',
    u'TORINO',
    u'CESENA',
    u'SASSARI',
    u'SANITA',
    u'BOLZANO',
    u'TRENTO',
    u'FIRENZE',
    u'MONOPOLI',
    u'STATO',
    u'AZIENDA',
    u'AZIENDE',
    u'SEZIONE',
    u'ENGINEERING',
    u'RICAMBI',
    u'COSTRUZIONI',
    u'INDUSTRIALE',
    u'SMITHKLINE',
    u'CONSULENZA',
    u'PROGETTAZIONE',
    u'RESTAURI',
    u'LAVORAZIONE',
    u'SOLUZIONI',
    u'ITALIA',
    u'CELL',
    u'LIQUIDE',
    u'NOVARTIS',
    u'VACCINES',
    u'ELECTROTEX',
    u'CALZATURIFICIO',
    u'PROD',
    u'MICROELECTRINICS',
    u'PROGETTI',
    u'ELETTROMECCANICA',
)
full_stop_list = set(stop_words) | set(dont_know) | set(e1_uni_sw) | set(e1_enti_sw)
aziende = set()
altridi = set()
possibili = wtr['DI']
for prova in possibili:
    if not 'DI' in d[prova]:
        print 'error  not DI in',prova," > ",d[prova]
        continue
    bow = d[prova]
    idx = bow.index('DI')
    if idx>1:
        tentativo = bow[idx+1:]
        print tentativo
        isName=False
        for word in tentativo:
            if word in stop_words or word not in names:
                isName=False
                break
            isName=True
        if isName:
            aziende.add(prova)
        else:
            altridi.add(prova)
db_save_table(c,aziende,'T_1__euristica_2_firms')
db_save_table(c,altridi,'T_1__euristica_2_altri')
estratti |= set(aziende)
estratti |= set(altridi)
stopped = dict([ (k,v) for k,v in d.items() if k not in estratti and reduce(lambda x,y: x or (y in full_stop_list),v,False)])
db_save_table(c,stopped,'T_1__euristica_2_stopped')
estratti |= set(stopped.keys())
e1_persone_titles = (
    u'ING',
    u'DR',
    u'PROF',
    u'DOTT',
)
stopped = dict([ (k,v) for k,v in d.items() if k not in estratti and reduce(lambda x,y: x or (y in e1_persone_titles),v,False)])
db_save_table(c,stopped,'T_1__euristica_3_persone_titles')
estratti |= set(stopped.keys())
residuals1 = dict([ (k,v) for k,v in d.items() if  k not in estratti])
db_save_table(c,residuals1,'residuals1')
k1 = [ (k,' '.join(v)) for k,v in d.items() if k not in estratti and not reduce(lambda x,y: x and (len(y)<2 or (y in names)),v)]
d1,wtr1,wall1 = loadall(k1,[])
db_mk_support_tables(c)

name = 'residuals'
if allTables:
    print "Saving ZZ_wtr"
    for k,v in wtr1.iteritems():
        res = [ (w,k) for w in v ]
        c.executemany("""INSERT INTO support4(id1,val1) VALUES (%s,%s)""", res)
    db_rename_table(c,'support4','T_%s_ZZ_wtr' % name )

    c.execute('drop view if exists residual_words')
    c.execute('create view residual_words as select val1,count(id1) from T_%s_ZZ_wtr group by val1 order by count(ID1) desc limit 100;' % name)

c.execute('drop table if exists _enti')
c.execute('create table _enti as select * from T_0__enti union select * from T_1__enti union select * from T_1__euristica_1_enti ' )

c.execute('drop table if exists _uni')
c.execute('create table _uni as select * from T_0__uni union select * from T_1__uni union select * from T_1__euristica_1_uni ' )

c.execute('drop table if exists _firms')
c.execute('create table _firms as select * from T_0__firms union select * from T_1__firms union select * from T_1__firms_w_amp union select * from T_1__euristica_2_firms union select * from T_1__euristica_2_stopped ' )

c.execute('drop table if exists _person')
c.execute('create table _person as select * from T_1__person union select * from residuals1 union select * from T_1__euristica_3_persone_titles ' )

c.execute('drop table if exists _unknown')
c.execute('create table _unknown as select * from T_1__euristica_1_dont_know union select * from T_1__euristica_2_altri ' )



b = [
    'base','enti','uni','firms','person','unknown'
    ]
a = {}
c.execute("""SELECT id1 FROM base""")
a[0] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _enti""")
a[1] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _uni""")
a[2] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _firms""")
a[3] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _person""")
a[4] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _unknown""")
a[5] = set(*zip(*c.fetchall()))

for i in range(1,6):
    for j in range(1,6):
        if i==j:
            continue
        A = a[i] & a[j]
        if A:
            print b[i],' & ',b[j],'=',A
            if i<j:
                for a1 in A:
                    c.execute('DELETE FROM _%s WHERE id1=%s' % (b[j],a1))
        O = a[i] - a[j]
        if O:
            print b[i],' - ',b[j],'=',len(O)
        O = a[j] - a[i]
        if O:
            print b[j],' - ',b[i],'=',len(O)


residuals = a[0] - ( a[1] | a[2] | a[3] | a[4] | a[5])



c.execute('drop table if exists _all')
c.execute('update _unknown set id3 = 0')
c.execute('update _enti set id3 = 1')
c.execute('update _uni set id3 = 2')
c.execute('update _firms set id3 = 3')
c.execute('update _person set id3 = 4')
c.execute('create table _all as select * from _enti union select * from _uni union select * from _firms union select * from _person union select * from _unknown  ' )
c.execute('alter table _all add primary key person_id (id1)')

c.execute('drop table if exists otherdb.patstat_names')
c.execute('create table otherdb.patstat_names as select id1,id3,val1 from _all' )

c.execute('drop table if exists _difference1')
c.execute('create table _difference1 as select a.* from _all a left join base b using(id1) where b.id1 is NULL    ' )
c.execute('drop table if exists _difference2')
c.execute('create table _difference2 as select b.* from base b left join _all a using(id1)  where a.id1 is NULL   ' )

cleanup(c)


### TEST phase


b = [
    'base','enti','uni','firms','person','unknown'
    ]
a = {}
c.execute("""SELECT id1 FROM base""")
a[0] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _enti""")
a[1] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _uni""")
a[2] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _firms""")
a[3] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _person""")
a[4] = set(*zip(*c.fetchall()))
c.execute("""SELECT id1 FROM _unknown""")
a[5] = set(*zip(*c.fetchall()))

for i in range(1,6):
    for j in range(1,6):
        if i==j:
            continue
        A = a[i] & a[j]
        if A:
            print b[i],' & ',b[j],'=',A
            if i<j:
                for a1 in A:
                    c.execute('DELETE FROM _%s WHERE id1=%s' % (b[j],a1))
        O = a[i] - a[j]
        if O:
            print b[i],' - ',b[j],'=',len(O)
        O = a[j] - a[i]
        if O:
            print b[j],' - ',b[i],'=',len(O)


residuals = a[0] - ( a[1] | a[2] | a[3] | a[4] | a[5] )

print "#residuals=",len(residuals),residuals

db_save_table(c,residuals,'_residuals')

print "END programs"
