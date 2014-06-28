import pymysql
import csv
import pdb
from app import app, host, port, user, passwd, db


#conect
conn = pymysql.connect(user=user, passwd=passwd, host=host, port=port)
db = conn.cursor()
# create db 
db.execute('CREATE DATABASE IF NOT EXISTS news_graph')
#connect db
db.execute('USE news_graph')

#delete tables if exists
db.execute('DROP TABLE IF EXISTS nodes, arcs')
#create tables
db.execute('''CREATE TABLE nodes
    (
      node_id INT NOT NULL PRIMARY KEY,
      label VARCHAR(30) NOT NULL UNIQUE,
      alexa INT NOT NULL,
      location VARCHAR(30) NOT NULL,
      n_out INT,
      n_in INT,
      w_out INT,
      w_in INT,
      w_diff INT,
      w_self INT,
      p_rank FLOAT
    )
''')
db.execute('''CREATE TABLE arcs
    (
        arc_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        source INT NOT NULL,
        target INT NOT NULL,
        weight_n1 FLOAT,
        weight_n2 FLOAT,
        weight_r INT
    )
''')

#save data to tables
csv_data = csv.reader(open('db_load/index_news_out2.csv', 'rb'))

i = 0
for row in csv_data:
    if i == 0:
        i = i+1
        continue
    #pdb.set_trace()
    db.execute('''INSERT INTO nodes(node_id, label, alexa, location, n_out, n_in, w_out, w_in, w_diff, w_self, p_rank) 
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )''', 
          row)

csv_data = csv.reader(open('db_load/arcs_news_out2.csv', 'rb'))
i=0
for row in csv_data:
    if i == 0:
        i = i+1
        continue
    db.execute('''INSERT INTO arcs(source, target, weight_n1, weight_n2, weight_r) 
          VALUES(%s, %s, %s, %s, %s)''', 
          row)
conn.commit()
