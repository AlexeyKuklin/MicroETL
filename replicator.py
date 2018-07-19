# -*- coding: utf-8 -*- 
import cx_Oracle
import pyodbc
import psycopg2
import io
from multiprocessing import Pool
import csv
import datetime as dt
import re
import json5 as json
import sys

def removekey(d, key):
    r = dict(d)
    del r[key]
    return r


def connection(workflow, name):
    conn = None
    c = workflow['connections'][name]
    t = c['type']
    cn = removekey(c, 'type')
    if t == 'Oracle':
        if 'dsn' in cn:
            cn['dsn'] = workflow['dsn'][c['dsn']]
    elif t == 'MSSQL':

        driver = c['driver']
        server = c['server'] 
        port = c['port']
        database = c['database']
        trusted_connection = c['trusted_connection'] 
        username = c['username'] 
        password = c['password']

        cstr = 'DRIVER={'+driver+'};SERVER='+server
        if len(port) > 0:
            cstr = cstr+':'+port
 
        cstr = cstr+';DATABASE='+database+';'

        if trusted_connection == 'yes':
            cstr = cstr+'trusted_connection=yes'
        else:
            cstr = cstr+'UID='+username+';PWD='+password
   
        print(cstr)

        cn = cstr    

    return cn, t


def get_connection(w):
    workflow, name = w
    cn, t = connection(workflow, name);
    print(cn)

    if t == 'Oracle':
        try:
            conn = cx_Oracle.connect(**cn)
        except cx_Oracle.Error as e:
            print(e)
            raise ValueError('Error Oracle connection') 
    elif t == 'PostgresQL':
        conn = psycopg2.extensions.connect(**cn)
    elif t == 'MSSQL':
        conn = pyodbc.connect(cn)
    elif t == 'csv':
        #read - write
        None 
    elif t == 'xsl':
        #read - write
        None 
    else:
         raise ValueError('Error connection type')
    return conn


def get_cursor(conn):
    try:
        return conn.cursor()   
    except e:
        print(e)
        raise ValueError('Error cursor')


def db_db(w):
    workflow, task = w
    print(task['id'])
    src_conn = get_connection((workflow, task['src_conn']))
    src_curs = get_cursor(src_conn)

    dst_conn = get_connection((workflow, task['dst_conn']))
    dst_curs = get_cursor(dst_conn)

    
    sel = ''
    if len(task['src_query']) > 0:
        sel = task['src_query']
    else:
        sel = "select * from " + task['src_table']

    print(sel)
    src_curs.execute(sel)
  
    t = type(dst_conn)
    if t == cx_Oracle.Connection: #Oracle 
        column_names = [i[0] for i in src_curs.description]
        cn = ', '.join(column_names)
        vs = ', '.join([':'+str(i+1) for i in range(len(src_curs.description))])
        to = 'insert into ' + task['dst_table'] + '(' + cn + ') values (' + vs +')'
        
        dst_curs.prepare(to)
        to_ex  = None

        db_types = (d[1] for d in src_curs.description)
        dst_curs.setinputsizes(*db_types)
    elif t == pyodbc.Connection: #MSSQL
        column_names = [i[0] for i in src_curs.description]
        cn = ', '.join(column_names)
        vs = ', '.join(['?' for i in range(len(src_curs.description))])
        to = 'insert into ' + task['dst_table'] + '(' + cn + ') values (' + vs +')'
        to_ex = to
        db_types = (d[1] for d in src_curs.description)
        dst_curs.setinputsizes(*db_types)
    elif t == psycopg2.extensions.connection: #PostgresQL
        to = "COPY " + obj['table_to'] + " FROM STDIN WITH CSV QUOTE '^' DELIMITER '\t' NULL ''"        
        #?
        db_types = (d[1] for d in src_curs.description)
        dst_curs.setinputsizes(*db_types)
    else:
        raise ValueError('Error set')
    

    print(to)

    
    rows, i = True, 0
    t0 = dt.datetime.now()          
    while rows:
        t1 = dt.datetime.now()       
        rows = src_curs.fetchmany(task['chunk_size'])
        t2 = dt.datetime.now()       
        if rows:
            if t == psycopg2.extensions.connection: #PostgresQL
                f = io.StringIO()
                wr = csv.writer(f, delimiter='\t', quotechar="^", quoting=csv.QUOTE_MINIMAL) 
         
                for r in rows:
                    wr.writerow(r)
         
                f.seek(0)
                dst_conn.copy_expert(to, f)
            else:
                dst_curs.executemany(to_ex, rows)

            t3 = dt.datetime.now()
            dst_conn.commit()
            t4 = dt.datetime.now()

            print('object=', task['id'], 'chunk=', i, 'fetch=',   (t2-t1).total_seconds(), 
                                                      'insert=',  (t3-t2).total_seconds(),
                                                      'commit=',  (t4-t3).total_seconds(),
                                                      'total=',   (t4-t0).total_seconds())            
            i += 1
    


def get_csv_lines(reader, chunk_size):
    lines = []
    for line in reader:
        lines.append(line)

        if len(lines) == chunk_size:
            return lines
            
    return lines 
    

def csv_db(w):
    workflow, task = w
    print(task['id'])

    dst_conn = get_connection((workflow, task['dst_conn']))
    dst_curs = get_cursor(dst_conn)

    reader = csv.reader(open(task['csv_file'], 'r'), 
                        delimiter=task['delimiter'], 
                        quotechar=task['quotechar'])
    header = next(reader)
    print('HEADER:')
    print(header)

    formats = task['format']
    print(formats)

    t = type(dst_conn)
    if t == cx_Oracle.Connection: #Oracle 
        #vs = ', '.join([':'+str(i+1) for i in range(len(header))])
        vs = []
        for n, va in enumerate(header):
            print(n, va)  
            if va in formats:
                vs.append('to_date(:'+str(n+1)+", '"+formats[va]+"')")
            else:
                vs.append(':'+str(n+1))
        vs = ', '.join(vs)

        cn = ', '.join(header)
        to = 'insert into ' + task['dst_table'] + '(' + cn + ') values (' + vs +')'
        dst_curs.prepare(to)
        to_ex  = None 
    elif t == pyodbc.Connection: #MSSQL
        cn = ', '.join(header)
        vs = ', '.join(['?' for i in range(len(header))])
        to = 'insert into ' + task['dst_table'] + '(' + cn + ') values (' + vs +')'
        to_ex = to


    elif t == psycopg2.extensions.connection: #PostgresQL
        raise ValueError('Not implemented yet')
    else:
        raise ValueError('Error set')
    
    c = True
    while c:
        lines = get_csv_lines(reader, task['chunk_size'])
        if len(lines) == 0:
            c = False
        else:
            dst_curs.executemany(to_ex, lines)    
            dst_conn.commit()
    


def format_date(d, format):
   #format = 'MM-DD-YYYY HH24:MI:SS'
   td = {
      'YYYY': '%Y',
      'YY'  : '%y',
      'MM'  : '%m',
      'DD'  : '%d',
      'HH24': '%H',
      'MI'  : '%M',
      'SS'  : '%S'
   }

   pattern = re.compile(r'\b(' + '|'.join(td.keys()) + r')\b')
   res = pattern.sub(lambda x: td[x.group()], format)
   return d.strftime(res)


def db_csv(w):
    workflow, task = w
    print(task['id'])
    src_conn = get_connection((workflow, task['src_conn']))
    src_curs = get_cursor(src_conn)
    
    sel = ''
    if len(task['src_query']) > 0:
        sel = task['src_query']
    else:
        sel = "select * from " + task['src_table']

    print(sel)
    src_curs.execute(sel)
    column_names = [i[0] for i in src_curs.description]
    print(column_names)

    delimiter = task['delimiter']
    quotechar = task['quotechar']

    formats = task['format']

    fo = open(task['csv_file'], 'w')

    if task['header'] != 'False':
        fo.write(delimiter.join(column_names)+'\n')

    rows, i = True, 0
    t0 = dt.datetime.now()          
    while rows:
        t1 = dt.datetime.now()       
        rows = src_curs.fetchmany(task['chunk_size'])
        t2 = dt.datetime.now()       
        if rows:
            f = io.StringIO()
            wr = csv.writer(f, delimiter=delimiter,
                               quotechar=quotechar,
                               quoting=csv.QUOTE_MINIMAL) 
       
            for r in rows:
                r = list(r)
                for k, v in formats.items():
                    i = column_names.index(k)
                    r[i] = format_date(r[i], v)
                wr.writerow(r)
         
            f.seek(0)
            
            t3 = dt.datetime.now()

            fo.write(f.getvalue())

            t4 = dt.datetime.now()

            print('object=', task['id'], 'chunk=', i, 'fetch=',   (t2-t1).total_seconds(), 
                                                      'write mem=',  (t3-t2).total_seconds(),
                                                      'write file=',  (t4-t3).total_seconds(),
                                                      'total=',   (t4-t0).total_seconds())            
            i += 1


def main():
    #pool = Pool(workflow['max_tasks'])
    #pool.map(db_db, workflow['tasks'])

    workflow = json.load(open('wf.json', 'r'))
    task = workflow['tasks'][0]

    try:
        f = getattr(sys.modules[__name__], task["type"])
        f((workflow, task))
    except e:
        raise ValueError('Unknown task type')


if __name__ == "__main__":
    main()
