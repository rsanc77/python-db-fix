#!/bin/env python

import ConfigParser
import MySQLdb
import re
from multiprocessing import Pool


class DatabaseTable:
    
    def __init__(self, host, user, passwd, dbase):
        try:
            self.__db = MySQLdb.connect(host, user, passwd, dbase)
            self.cursor = self.__db.cursor()
        except MySQLdb.Error, e:
            print "MySQL Error [{0}]: {1}".format(e.args[0], e.args[1])
        
    def processTableColumns(self, table):
        #print table
        tq= "SHOW FULL COLUMNS FROM `{0}` WHERE COLLATION LIKE 'utf8_%'".format(table)
        try:
            self.cursor.execute(tq)
            tdata = self.cursor.fetchall()
        except MySQLdb.Error, e:
            print "MySQL Error [{0}]: {1} in table {2}".format(e.args[0], e.args[1],table)
            
        for t in tdata:
            if re.match('text', t[1]):
                self.changeTable(table, t[0], 'TEXT', 'BLOB')
                
            if re.match('longtext', t[1]):
                self.changeLTTable(table, t[0], 'LONGTEXT', 'LONGBLOB')
          
            if re.match('mediumtext', t[1]):
                self.changeTable(table, t[0], 'MEDIUMTEXT', 'MEDIUMBLOB')
                
            if re.match('tinytext', t[1]):
                self.changeTable(table, t[0], 'TINYTEXT', 'TINYBLOB')
           
            if re.match('varchar', t[1]):
                self.changeTable(table, t[0], t[1], re.sub('varchar', 'varbinary', t[1]))
        #self.cursor.execute("FLUSH TABLE {0}".format(table))           
                 
    def changeTable(self, table, column, ctype, cbintype):
        try:
		    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} CHARACTER SET latin1 COLLATE latin1_general_ci;""".format(table, column, ctype))
		    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2};""".format(table,column,cbintype))
		    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} COLLATE utf8_unicode_ci;""".format(table, column, ctype))
        except MySQLdb.Error, e:
            print "MySQL error [{0}]: {1} in table {2}".format(e.args[0], e.args[1],table)
            pass

	    
    def changeLTTable(self, table, column, ctype, cbintype):
        q = """SHOW INDEX FROM {0} WHERE Index_type = 'FULLTEXT'""".format(table)
        ftext = self.cursor.execute(q)
        fdata = self.cursor.fetchall()
        for x in fdata:
            key_name = x[2]
        try:
            if ftext:
			    self.cursor.execute("""DROP INDEX `{0}` ON `{1}`;""".format(key_name, table))
			    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} CHARACTER SET latin1 COLLATE latin1_general_ci;""".format(table, column, ctype))
			    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2};""".format(table,column,cbintype))
			    self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} COLLATE utf8_unicode_ci;""".format(table, column, ctype))
			    self.cursor.execute("""CREATE FULLTEXT INDEX {1} ON {0}({1});""".format(table,key_name))
                
            else:
                self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} CHARACTER SET latin1 COLLATE latin1_general_ci;""".format(table, column, ctype))
                self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2};""".format(table,column,cbintype))
                self.cursor.execute("""ALTER TABLE `{0}` CHANGE `{1}` `{1}` {2} COLLATE utf8_unicode_ci;""".format(table, column, ctype))
        except MySQLdb.Error, e:
            print "Mysql Error [{0}]: {1} in table {2}".format(e.args[0], e.args[1],table)
            pass 

	        
    def __del__(self):
        class_name = self.__class__.__name__
       
       
def get_config(conf_file):
	conf = ConfigParser.ConfigParser()
	conf.read(conf_file)
	res = []
	
	res.insert(0, conf.get('database','host'))
	res.insert(1, conf.get('database','user'))
	res.insert(2, conf.get('database','passw'))
	res.insert(3, conf.get('database','dbase'))
	res.insert(4, conf.get('database','poolSize'))

	return res

conf = get_config("./settings.ini")

def db_tables():         
    db = MySQLdb.connect(conf[0], conf[1], conf[2], conf[3])


    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    tables = []
    try:
        cursor.execute("SHOW TABLES")
        data = cursor.fetchall()
        for t in data:
            if t[0] != '':
                tables.append(t[0])
    except MySQLdb.Error, e:
        print "MySQL Error [{0}]: {1}".format(e.args[0], e.args[1])
    
    db.close()
    return tables
 
def  table_worker(table):
    test = DatabaseTable(conf[0], conf[1], conf[2], conf[3])
    test.processTableColumns(table)


def main():
	try:
		tables = db_tables()
		p = Pool(int(conf[4]))
		p.map(table_worker, tables)
	except Exception as e:
		print "{0}".format(e)
	
if __name__=='__main__':
    main()
