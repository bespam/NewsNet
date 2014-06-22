from flask import render_template, flash, redirect, url_for, session
from app import app, host, port, user, passwd, db
import pymysql
from forms import PosForm, NegForm, SlidersForm
import json
import pdb
import math, random
from sets import Set
import numpy as np


# ROUTING/VIEW FUNCTIONS
remote = 1


@app.route('/')
@app.route('/index')
@app.errorhandler(500)
@app.errorhandler(404)
@app.route('/search', methods = ['GET', 'POST'])
def search():
    # Renders index.html.
    db = connect_db(remote = remote)
    #connect db
    db.execute('USE news_graph')
    
    #get list of all domains
    sql_q = ''' SELECT label, alexa from nodes
            ORDER BY alexa ASC'''
    db.execute(sql_q)    
    tuples = db.fetchall()    
    all_domains = [{'name':v[0]} for v in tuples]
    #generate forms    
    pos_form = PosForm(csrf_enabled = False)
    neg_form = NegForm(csrf_enabled = False)
    sliders_form = SlidersForm(csrf_enabled = False)
    #dafault values for sliders
    if "config" in session:
        config = session["config"] 
    else:
        config = {'neg_domains':[],'pos_domains':[], 'links_slider':'40','alexa_slider':'40', 'p_rank_slider':'40',
                'in_slider':'0','out_slider':'0','self_slider':'0'} 
    rank_sel =[int(config['links_slider']), int(config['alexa_slider']),int(config['p_rank_slider']),
        int(config['in_slider']),int(config['out_slider']),int(config['self_slider'])]          
    #add pos domain 
    if pos_form.data["add_pos_domain"]:
        domain = pos_form.data["add_pos_domain"]        
        if domain not in config["pos_domains"]+config["neg_domains"]:
            config['pos_domains'].append(domain)
    #add neg domain
    if neg_form.data["add_neg_domain"]:
        domain = neg_form.data["add_neg_domain"]        
        if domain not in config["pos_domains"]+config["neg_domains"]:
            config['neg_domains'].append(domain)
    #delete pos domain   
    if pos_form.data["del_pos_domain"]:
        domain = pos_form.data["del_pos_domain"]
        config['pos_domains'].remove(domain)
    #delete neg domain  
    if neg_form.data["del_neg_domain"]:
        domain = neg_form.data["del_neg_domain"]
        config['neg_domains'].remove(domain)         
    #sliders change 
    if sliders_form.data['links_slider']:
        rank_sel = [int(sliders_form.data['links_slider']), int(sliders_form.data['alexa_slider']), 
        int(sliders_form.data['p_rank_slider']),int(sliders_form.data['in_slider']),
        int(sliders_form.data['out_slider']),int(sliders_form.data['self_slider'])]
        config['links_slider']=str(rank_sel[0])
        config['alexa_slider']=str(rank_sel[1])
        config['p_rank_slider']=str(rank_sel[2])
        config['in_slider']=str(rank_sel[3])
        config['out_slider']=str(rank_sel[4])
        config['self_slider']=str(rank_sel[5])        
    session["config"] = config
    if len(config['neg_domains']) == 0 and len(config['pos_domains']) == 0:     
        return render_template('search.html',
                pos_form = pos_form,
                neg_form = neg_form,
                sliders_form = sliders_form,
                all_domains = all_domains,
                config = config)

    arcs, sel_pos_nodes, sel_neg_nodes, rec_nodes = query_db(db, config["pos_domains"], config["neg_domains"])
    data, graph = process(arcs, sel_pos_nodes, sel_neg_nodes, rec_nodes, config["pos_domains"], config["neg_domains"], np.array(rank_sel)/float(sum(rank_sel)))                 
    return render_template('search.html',
        pos_form = pos_form,
        neg_form = neg_form,
        sliders_form = sliders_form,
        all_domains = all_domains,
        config = config,
        data = data,
        graph = graph)  

@app.route('/explore')
def explore():
    # Renders explore.html.
    return render_template('explore.html')

@app.route('/table')
def table():
    # Renders table.html.
    #connect db
    db = connect_db(remote = remote)
    db.execute('USE news_graph')  
    #get list of all domains
    sql_q = ''' SELECT * from nodes
            ORDER BY alexa ASC'''
    db.execute(sql_q)    
    out = db.fetchall()
    table = []
    for line in out:
       table.append([line[1],line[3],line[2], "%.4f" % line[10], line[7], line[6], line[9]])           
    return render_template('table.html', table=table)

@app.route('/slides')
def slides():
    # Renders slides.html.
    return render_template('slides.html')

@app.route('/author')
def author():
    # Renders author.html.
    return render_template('author.html')



#additional functions

#convert UTF json to ascii json
def ascii_encode_dict(data):
    ascii_encode = lambda x: x.encode('ascii')
    return dict(map(ascii_encode, pair) for pair in data.items())

#connection to db
def connect_db(remote):
    if remote:
        #remote
        conn = pymysql.connect(user='bespam', passwd='12345678', host='insight.cv1io5wgnzkw.us-west-1.rds.amazonaws.com', port=3306)
    else:
        #local
        conn = pymysql.connect(user='root', passwd='12345678', host='localhost')
    return conn.cursor()


#queryDB
def query_db(db, pos_domains, neg_domains):
    pos_domains_str = '"'+'", "'.join(pos_domains+["1"])+'"'
    neg_domains_str = '"'+'", "'.join(neg_domains+["1"])+'"'
    joined_domains_str = '"'+'", "'.join(pos_domains + neg_domains+["1"])+'"'
    #get all the relevant arcs
    sql_q = '''SELECT DISTINCT arc_id, source, target, weight_n1 from arcs join (SELECT node_id FROM nodes WHERE label IN (''' +joined_domains_str+''')) as A ON arcs.source = A.node_id OR arcs.target = A.node_id'''      
    db.execute(sql_q)    
    arcs = db.fetchall()
    #get all the relevant pos nodes
    sql_q = '''SELECT * from nodes WHERE label IN (''' +pos_domains_str+''') 
        ORDER BY alexa ASC'''      
    db.execute(sql_q)    
    sel_pos_nodes = db.fetchall()
    #get all the relevant neg nodes
    sql_q = '''SELECT * from nodes WHERE label IN (''' +neg_domains_str+''') 
        ORDER BY alexa ASC'''      
    db.execute(sql_q)    
    sel_neg_nodes = db.fetchall()
    #get recommendation
    sql_q = '''SELECT node_id,label, alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank, SUM(amount) as am FROM
            ((SELECT * from (
                SELECT * from nodes 
                JOIN
                (SELECT col, SUM(weight_n2) as amount FROM (
                    (SELECT source as col, weight_n2 from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +pos_domains_str+''')) as A 
                        ON arcs.target = A.node_id
                    ) UNION
                    (SELECT target as col, weight_n2 from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +pos_domains_str+''')) as B 
                        ON arcs.source = B.node_id
                    )) as T
                    GROUP BY col
                ) as E
                on nodes.node_id = E.col
                ORDER BY amount DESC
                LIMIT 100) as G 
            WHERE label NOT IN ('''+joined_domains_str+ '''))
            UNION
            (SELECT * from (
                SELECT * from nodes 
                JOIN
                (SELECT col, -SUM(weight_n2) as amount FROM (
                    (SELECT source as col, weight_n2 from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +neg_domains_str+''')) as A 
                        ON arcs.target = A.node_id
                    ) UNION
                    (SELECT target as col, weight_n2 from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +neg_domains_str+''')) as B 
                        ON arcs.source = B.node_id
                    )) as T
                    GROUP BY col
                ) as E
                on nodes.node_id = E.col
                ORDER BY amount ASC
                LIMIT 10000) as G 
            WHERE label NOT IN ('''+joined_domains_str+ '''))
            ) as j
            GROUP BY node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank
            ORDER BY am DESC
            '''    
    db.execute(sql_q)
    out = db.fetchall()    
    rec_nodes= np.array(out)
    return arcs, sel_pos_nodes, sel_neg_nodes, rec_nodes

#generate recommended list and subgraph
def process(arcs, sel_pos_nodes, sel_neg_nodes, rec_nodes, pos_domains, neg_domains, rank_sel):
    #create number of links column
    rec_joined = np.ones((len(rec_nodes),1))/10.0
    h = len(rec_joined)
    rec_joined = np.append(rec_joined,rec_nodes,axis=1)
    #renormalizing values
    #inverse_alexa_ranking
    rec_joined[:,3] = max(rec_joined[:,3].astype('float'))/(rec_joined[:,3].astype('float'))   
    #log norm
    rec_joined[:,11]=np.array(rec_joined[:,11].astype('float')/min(rec_joined[:,11].astype('float')))
    rec_joined[:,8]=np.array(rec_joined[:,8].astype('float')+1.0) 
    rec_joined[:,7]=np.array(rec_joined[:,7].astype('float')+1.0) 
    rec_joined[:,10]=np.array(rec_joined[:,10].astype('float')+1.0)     
 
    #Select top 20 based on the cumulative metric
    rec_max = np.amax(rec_joined[:,[0,3,11,8,7,10]].astype('float'),0)
    rec_min = np.amin(rec_joined[:,[0,3,11,8,7,10]].astype('float'),0)
    #fix rec_min problem
    rec_min[0] = 0.0
    rec_max[0] = 1.0    
    #combined rank
    #pdb.set_trace()
    r = np.sum((rec_joined[:,[0,3,11,8,7,10]].astype('float')-rec_min)/(rec_max-rec_min)*rank_sel,1)*rec_joined[:,12].astype('float')
    #normalize final rank
    if max(r) <=0:
        r2 = r*0.0
    else:
        #r1 = np.array((np.array([i if i>0 else 0 for i in r])+max(r.tolist())*0.1).tolist())    
        r2 = r#(r-min(r))/(max(r)-min(r))
    #pdb.set_trace()
    #pdb.set_trace() 
    #combine arrays
    data = np.append(rec_nodes,np.matrix(r2).T,axis=1)
    #pdb.set_trace()    
    #sort on the rank
    data_sorted = sorted(np.array(data), key = lambda x: -float(x[12]))
    
    #generate graph
    graph = {"nodes":[], "edges":[]}
    node_ids = []
    max_size = 20
    min_size = 1
    #extract all pos nodes for selected pos domains
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank in sel_pos_nodes:
        node_ids.append(str(node_id))
        graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":10, "color":"#5cb85c"}) 
    #extract all pos nodes for selected neg domains
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank in sel_neg_nodes:
        node_ids.append(str(node_id))
        graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":10, "color":"#d9534f"})   
    #pdb.set_trace()
    #normalize colors
    i = 0
    recs = []
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank, w_conn,rec in data_sorted:   
        if label not in pos_domains+neg_domains:        
            i = i + 1
            recs.append(float(rec))
            if i == 20: break          
    #extract all nodes for connected domains
    #give 20 recommendations  
    i = 0
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank, w_conn,rec in data_sorted:
        #only recommend domains not in the joined domain list        
        if label not in pos_domains+neg_domains:
            i = i + 1
            node_ids.append(str(node_id))
            #shade all the recommendation if no pos_domains
            if len(pos_domains) == 0:
                rec_col = 0.01
            else:
                rec_col = (float(rec)-min(recs))/(max(recs)-min(recs))
            col = "#"+str(int(10-float(rec_col)*10.0))+str(int(10-float(rec_col)*10.0))+"F"
            graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":(max_size-min_size)*float(rec_col)+min_size, "color":col})
            if i == 20: break   
    
    for arc_id,source,target,weight_n2 in arcs:
        #pdb.set_trace()
        if str(source) in node_ids and str(target) in node_ids:
            graph["edges"].append({"id":str(arc_id),"source":str(source),"target":str(target), 
            "weight":float(weight_n2), "type":"curvedArrow"})     
    return data_sorted[0:20], graph



