from flask import render_template, flash, redirect, url_for, request
from app import app, host, port, user, passwd, db
from app.helpers.database import con_db
import pymysql
from forms import EditForm, SliderForm
import json
import pdb
import math, random
from sets import Set
import numpy as np


# To create a database connection, add the following
# within your view functions:
# con = con_db(host, port, user, passwd, db)


# ROUTING/VIEW FUNCTIONS
@app.route('/')
@app.route('/index')
def index():
    # Renders search.html.
    return render_template('index.html')


@app.route('/search', methods = ['GET', 'POST'])
def search():
    # Renders index.html.
    #Local
    #conn = pymysql.connect(user='root', passwd='12345678', host='localhost')
    #remote
    conn = pymysql.connect(user='bespam', passwd='12345678', host='insight.cdu2bu8f4pau.us-east-1.rds.amazonaws.com', port=3306)
    
    db = conn.cursor()
    #connect db
    db.execute('USE news_graph')
    
    #get list of all domains
    sql_q = ''' SELECT label, alexa from nodes
            ORDER BY alexa ASC'''
    db.execute(sql_q)    
    tuples = db.fetchall()    
    all_domains = [{'name':v[0]} for v in tuples]
    #generate forms and get requests    
    add_form = EditForm(csrf_enabled = False)
    slider_form = SliderForm(csrf_enabled = False)
    #dafault values for sliders
    if request.form.getlist('config'):
        rank_sel = [int(slider_form.data['links_slider']), int(slider_form.data['alexa_slider']), 
        int(slider_form.data['p_rank_slider']),int(slider_form.data['in_slider']),
        int(slider_form.data['out_slider']),int(slider_form.data['self_slider'])]
        config = json.loads(slider_form.data['config'].replace("'",'"'), object_hook=ascii_encode_dict)
        config['links_slider']=str(rank_sel[0])
        config['alexa_slider']=str(rank_sel[1])
        config['p_rank_slider']=str(rank_sel[2])
        config['in_slider']=str(rank_sel[3])
        config['out_slider']=str(rank_sel[4])
        config['self_slider']=str(rank_sel[5])        
    else:
        config = mod_config(request.form)
        rank_sel =[int(config['links_slider']), int(config['alexa_slider']),int(config['p_rank_slider']),
        int(config['in_slider']),int(config['out_slider']),int(config['self_slider'])]
    #process slider request or other process requests 
    #config = {'neg_domains':'cnn.com','pos_domains':'fox.com'}    
    data = []
    graph = []    
    if config['pos_domains'] != '' or config['neg_domains'] != '':        
        if config['pos_domains'] != '':
            pos_domains = '"'+'", "'.join(config['pos_domains'].split(", "))+'"'
        else:
            pos_domains = '1'            
        if config['neg_domains'] != '':
            neg_domains = '"'+'", "'.join(config['neg_domains'].split(", "))+'"'
        else:
            neg_domains = '1'
        joined_domains = '"'+'", "'.join(config['pos_domains'].split(", ")+config['neg_domains'].split(", "))+'"'
        print joined_domains 
        data, graph = query_db(db, pos_domains, neg_domains, joined_domains, rank_sel = np.array(rank_sel)/float(sum(rank_sel)))                 
        #data = pos_data
        return render_template('search.html',
            add_form = add_form,
            slider_form = slider_form,
            all_domains = all_domains,
            config = config,
            data = data,
            graph = graph)  
    return render_template('search.html',
        add_form = add_form,
        slider_form = slider_form,
        all_domains = all_domains,
        config = config)

@app.route('/explore')
def explore():
    # Renders explore.html.
    return render_template('explore.html')

@app.route('/slides')
def slides():
    # Renders slides.html.
    return render_template('slides.html')

@app.route('/author')
def author():
    # Renders author.html.
    return render_template('author.html')

@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500


#extract config from proper requests
def mod_config(r):
    pos_req1 = r.getlist('add_pos_domain')
    pos_req2 = r.getlist('pos_domains')
    pos_req3 = r.getlist('del_pos_domain')    
    neg_req1= r.getlist('add_neg_domain')
    neg_req2 = r.getlist('neg_domains')
    neg_req3 = r.getlist('del_neg_domain')
    if len(pos_req1) == 0 and len(neg_req1) == 0 and len(pos_req3) == 0 and len(neg_req3) == 0:
        config = {'neg_domains':'','pos_domains':'', 'links_slider':'100','alexa_slider':'10', 'p_rank_slider':'10',
                'in_slider':'10','out_slider':'10','self_slider':'10'}
        return config 
    if len(pos_req3) != 0:
        config = json.loads(pos_req3[0].replace("'",'"'), object_hook=ascii_encode_dict)
        pos_domains = config['pos_domains']
        pos_delete = config['pos_delete']
        p2 = pos_domains.split(", ")
        p2.remove(pos_delete)
        config['pos_domains'] = ", ".join(p2)
        config.pop('pos_delete')
        return config
    if len(neg_req3) != 0:
        config = json.loads(neg_req3[0].replace("'",'"'), object_hook=ascii_encode_dict)
        neg_domains = config['neg_domains']
        neg_delete = config['neg_delete']
        p2 = neg_domains.split(", ")
        p2.remove(neg_delete)
        config['neg_domains'] = ", ".join(p2)
        config.pop('neg_delete')        
        return config   
    if len(pos_req1) != 0:
        domain = pos_req1[0].encode()
        print pos_req2[0].replace("'",'"')
        config = json.loads(pos_req2[0].replace("'",'"'), object_hook=ascii_encode_dict)
        print config
        if config['pos_domains'] == '':
            config['pos_domains'] = domain
        else:
            config['pos_domains'] = config['pos_domains'] + ', '+ domain
        return config
    if len(neg_req1) != 0:
        domain = neg_req1[0].encode()
        config = json.loads(neg_req2[0].replace("'",'"'), object_hook=ascii_encode_dict)
        if config['neg_domains'] == '':
            config['neg_domains'] = domain
        else:
            config['neg_domains'] = config['neg_domains'] + ', '+ domain
        return config

#convert UTF json to ascii json
def ascii_encode_dict(data):
    ascii_encode = lambda x: x.encode('ascii')
    return dict(map(ascii_encode, pair) for pair in data.items())


#queryDB
def query_db(db, pos_domains, neg_domains, joined_domains, rank_sel):
    #get all the relevant arcs
    sql_q = '''SELECT DISTINCT arc_id, source, target, weight_n from arcs join (SELECT node_id FROM nodes WHERE label IN (''' +joined_domains+''')) as A ON arcs.source = A.node_id OR arcs.target = A.node_id'''      
    db.execute(sql_q)    
    arcs = db.fetchall()
    #get all the relevant pos nodes
    sql_q = '''SELECT * from nodes WHERE label IN (''' +pos_domains+''') 
        ORDER BY alexa ASC'''      
    db.execute(sql_q)    
    sel_pos_nodes = db.fetchall()
    #get all the relevant neg nodes
    sql_q = '''SELECT * from nodes WHERE label IN (''' +neg_domains+''') 
        ORDER BY alexa ASC'''      
    db.execute(sql_q)    
    sel_neg_nodes = db.fetchall()
    #get recommendation
    sql_q = '''SELECT node_id,label, alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank, SUM(amount) as am FROM
            ((SELECT * from (
                SELECT * from nodes 
                JOIN
                (SELECT col, SUM(weight_r) as amount FROM (
                    (SELECT source as col, weight_r from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +pos_domains+''')) as A 
                        ON arcs.target = A.node_id
                    ) UNION
                    (SELECT target as col, weight_r from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +pos_domains+''')) as B 
                        ON arcs.source = B.node_id
                    )) as T
                    GROUP BY col
                ) as E
                on nodes.node_id = E.col
                ORDER BY amount DESC
                LIMIT 100) as G 
            WHERE label NOT IN ('''+joined_domains+ '''))
            UNION
            (SELECT * from (
                SELECT * from nodes 
                JOIN
                (SELECT col, -SUM(weight_r) as amount FROM (
                    (SELECT source as col, weight_r from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +neg_domains+''')) as A 
                        ON arcs.target = A.node_id
                    ) UNION
                    (SELECT target as col, weight_r from arcs
                        JOIN 
                        (SELECT node_id FROM nodes WHERE label IN (
                             ''' +neg_domains+''')) as B 
                        ON arcs.source = B.node_id
                    )) as T
                    GROUP BY col
                ) as E
                on nodes.node_id = E.col
                ORDER BY amount ASC
                LIMIT 100) as G 
            WHERE label NOT IN ('''+joined_domains+ '''))
            ) as j
            GROUP BY node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank
            ORDER BY am DESC
            '''    
    db.execute(sql_q)    
    rec_nodes= np.array(db.fetchall())
    #create number of links column
    rec_joined = np.ones((len(rec_nodes), 1))
    rec_joined = np.append(rec_joined,rec_nodes,axis=1)
    #inverse_alexa_ranking
    rec_joined[:,3] = np.array(1.0/rec_joined[:,3])    
    #Select top 20 based on the cumulative metric
    rec_max = np.amax(rec_joined[:,[0,3,11,7,8,10]],0).astype('float')
    rec_min = np.amin(rec_joined[:,[0,3,11,7,8,10]],0).astype('float')
    #fix rec_min problem
    rec_min[0]=0.0
    #combined rank
    r = np.sum((rec_joined[:,[0,3,11,7,8,10]]-rec_min)/(rec_max-rec_min)*rank_sel,1)*rec_joined[:,12].astype('float')
    #normalize final rank
    if max(r) <=0:
        r2 = r*0.0+0.01
    else:
        r1 = np.log((np.array([i if i>0 else 0 for i in r])+max(r.tolist())*0.1).tolist())    
        r2 = (r1-min(r1))/(max(r1)-min(r1))
    #combine arrays
    data = np.append(rec_nodes,np.matrix(r2).T,axis=1)
    #sort on the rank
    data_sorted = sorted(np.array(data), key = lambda x: -x[12])
    
    #generate graph
    graph = {"nodes":[], "edges":[]}
    node_ids = []
    max_size = 20
    min_size = 1
    #extract all pos nodes for selected pos domains
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank in sel_pos_nodes:
        node_ids.append(node_id)
        graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":10, "color":"#5cb85c"}) 
    #extract all pos nodes for selected neg domains
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank in sel_neg_nodes:
        node_ids.append(node_id)
        graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":10, "color":"#d9534f"})     
    #extract all nodes for connected domains
    for node_id,label,alexa,location,n_out,n_in,w_out,w_in,w_diff,w_self,p_rank, s,rec in data_sorted[0:20]:
        #only recommend domains not in the joined domain list        
        if label not in joined_domains.replace('"','').split(", "):
            node_ids.append(node_id)
            col = "#"+str(int(10-rec*10.0))+str(int(10-rec*10.0))+"F"
            graph["nodes"].append({"id":str(node_id),"label":label,"x":random.randint(-100,100), "y":random.randint(-100,100),"size":(max_size-min_size)*rec+min_size, "color":col})   
    for arc_id,source,target,weight_n in arcs:
        if source in node_ids and target in node_ids:
            graph["edges"].append({"id":str(arc_id),"source":str(source),"target":str(target), 
            "weight":weight_n, "type":"curvedArrow"})        
    return data_sorted[0:20], graph



