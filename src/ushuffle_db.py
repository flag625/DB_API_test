#!/usr/bin/env python
# coding: utf-8

"""
   本脚本使用不同数据库（mySQL、SQLite和Gadfly）执行一些基础操作。
   在python2和python3下都可以执行。
   本代码参考Python核心编程（第3版），非本人原创代码。
"""

from distutils.log import warn as printf
import os, time
from random import randrange as rand

if isinstance(__builtins__, dict) and 'raw_input' in __builtins__:
    scanf = raw_input
elif hasattr(__builtins__, 'raw_input'):
    scanf = raw_input
else:
    scanf = input

COLSIZ = 10
FIELDS = ('login','userid','projid')
RDBMSs = {'s':'sqlite', 'm':'mysql', 'g':'gadfly'}
DBNAME = 'test'
DBUSER = 'root'
DB_EXC = None
NAMWLEN = 16

tformat = lambda s: str(s).title().ljust(COLSIZ)
cformat = lambda s: s.upper().ljust(COLSIZ)

def setup():
    return RDBMSs[input('''
    Choose a database system:
    
    (M)ySQL
    (G)Adfly
    (S)QLite
    
    Enter choice: ''').strip().lower()[0]]

def connect(db, DBNAME):
    global DB_EXC
    dbDir = '%s_%s' %(db, DBNAME)

    # 调用SQLite
    if db == 'sqlite':
        try:
            import sqlite3
        except ImportError:
            try:
                from pysqlite2 import dbapi2 as sqlite3
            except ImportError:
                return None
        DB_EXC = sqlite3
        if not os.path.isdir(dbDir):
            os.makedirs(dbDir)
        cxn = sqlite3.connect(os.path.join(dbDir, DBNAME))

    # 调用MySQL
    elif db == 'mysql':
        try:
            import MySQLdb
            import _mysql_exceptions as DB_EXC

            try:
                cxn = MySQLdb.connect(db=DBNAME)
            except DB_EXC.OperationalError:
                try:
                    cxn = MySQLdb.connect(user=DBUSER, passwd='a12345678')
                    cxn.query('create database %s' %DBNAME)
                    cxn.commit()
                    cxn.close()
                    cxn = MySQLdb.connect(db=DBNAME)
                except DB_EXC.OperationalError:
                    return None
        except ImportError:
            try:
                #import mysql.connector as my_sql
                #import mysql.connector.errors as DB_EXC
                import pymysql as my_sql
                import pymysql.err as DB_EXC
                try:
                    cxn = my_sql.connect(**{
                        'database':DBNAME,
                        'user'    :DBUSER,
                        'password':'a12345678'
                    })
                except DB_EXC.InterfaceError:
                    return None
            except ImportError:
                return None

    #调用Gadfly,暂时没有安装gadfly库
    elif db == 'gadfly':
        try:
            from gadfly import gadfly
            DB_EXC = gadfly
        except ImportError:
            return None
        try:
            cxn = gadfly(DBNAME, dbDir)
        except IOError:
            cxn = gadfly()
            if not os.path.isdir(dbDir):
                os.makedirs(dbDir)
            cxn.startup(DBNAME, dbDir)

    else:
        return None

    return cxn

def create(cur, retry=3):
    try:
        cur.execute('''
            create table users (
                login varchar(%d),
                userid integer,
                projid integer )
            ''' %NAMWLEN)
    except DB_EXC.OperationalError:
        if retry > 0:
            drop(cur)
            retry -= 1
            time.sleep(10)
            create(cur, retry)
        else:
            return None

drop = lambda cur: cur.execute('drop table users')

NAMES = (
    ('aaron',8312),('angela',7603),('dave',7306),
    ('davina',7902),('elliot',7911),('ernie',7410),
)

def randName():
    pick = set(NAMES)
    while pick:
        yield pick.pop()

def insert(cur, db):
    if db == 'sqlite':
        cur.executemany("insert into users values(?,?,?)",
                        [(who,uid,rand(1,5)) for who,uid in randName()])
    elif db == 'gadfly':
        for who, uid in randName():
            cur.execute("insert into users values(?,?,?)",
                        (who, uid, rand(1,5)))
    elif db == 'mysql':
        cur.executemany("insert into users values(%s,%s,%s)",
                        [(who,uid,rand(1,5)) for who, uid in randName()])

getRC = lambda cur: cur.rowcount if hasattr(cur, 'rowcount') else -1

def update(cur):
    fr = rand(1,5)
    to = rand(1,5)
    while(fr == to):
        to = rand(1,5)
    cur.execute("update users set projid = %d where projid = %d" %(to,fr))
    return fr, to, getRC(cur)

def delete(cur):
    rm = rand(1,5)
    if rm not in get_proid(cur):
        return rm, -1
    cur.execute('delete from users where projid = %d' %rm)
    return rm, getRC(cur)

def get_proid(cur):
    cur.execute('select distinct projid from users')
    return cur.fetchall()

def dbDump(cur):
    cur.execute('select * from users')
    printf('\n%s' %' '.join(map(cformat,FIELDS)))
    for data in cur.fetchall():
        printf(' '.join(map(tformat, data)))

def main():
    db = setup()
    printf('*** Connect to %r database' % db)
    cxn = connect(db, DBNAME)
    if not cxn:
        printf('ERROR: %r not supported or unreachable, exit' %db)
        return
    cur = cxn.cursor()

    printf('\n*** Creating users table')
    create(cur)

    printf('\n*** Inserting names into table')
    insert(cur, db)
    dbDump(cur)

    printf('\n*** Randomly moving folks')
    fr, to, num = update(cur)
    printf('\t(%d users moved) from (%d) to (%d)' %(num, fr, to))
    dbDump(cur)

    printf('\n*** Randomly chossing group')
    rm, num = delete(cur)
    printf('\t(group #%d; %d users removed)' % (rm, num))
    dbDump(cur)

    printf('\n*** Dropping users table')
    drop(cur)
    printf('\n*** Close cxns')
    cur.close()
    cxn.commit()
    cxn.close()

if __name__ == '__main__':
    main()






