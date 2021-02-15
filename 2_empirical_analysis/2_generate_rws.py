# -*- coding: utf-8 -*-
"""
@author: arvmar
"""



import networkx
import pandas as pd
import tensorflow
import numpy as np
import argparse
import json
import scipy
import random
import os



"""
FUNCTIONS
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
"""
def alpha(p,q,t,x,adj_mat_csr_sparse):
    if t==x:
        return 1.0/p
    elif adj_mat_csr_sparse[t,x]>0:
        return 1.0
    else:
        return 1.0/q
    
def compute_transition_prob(adj_mat_csr_sparse,p,q):
    
    ## Test
    #adj_mat_csr_sparse = mycr_mat
    #p = 0.5
    #q = 0.5
    
    transition={}
    num_nodes=adj_mat_csr_sparse.shape[0]
    indices=adj_mat_csr_sparse.indices
    indptr=adj_mat_csr_sparse.indptr
    data=adj_mat_csr_sparse.data
    #Precompute the transition matrix in advance
    for t in range(num_nodes):#t is row index
        for v in indices[indptr[t]:indptr[t+1]]:#i.e  possible next ndoes from t
            pi_vx_indices=indices[indptr[v]:indptr[v+1]]#i.e  possible next ndoes from v
            pi_vx_values = np.array([alpha(p,q,t,x,adj_mat_csr_sparse) for x in pi_vx_indices])
            pi_vx_values=pi_vx_values*data[indptr[v]:indptr[v+1]]
            #This is eqilvalent to the following
    #         pi_vx_values=[]
    #         for x in pi_vx_indices:
    #             pi_vx=alpha(p,q,t,x)*adj_mat_csr_sparse[v,x]
    #             pi_vx_values.append(pi_vx)
            pi_vx_values=pi_vx_values/np.sum(pi_vx_values)
            #now, we have normalzied transion probabilities for v traversed from t
            #the probabilities are stored as a sparse vector. 
            transition[t,v]=(pi_vx_indices,pi_vx_values)

    return transition

def generate_random_walks(adj_mat_csr_sparse,transition,random_walk_length):
    random_walks=[]
    num_nodes=adj_mat_csr_sparse.shape[0]
    indices=adj_mat_csr_sparse.indices
    indptr=adj_mat_csr_sparse.indptr
    data=adj_mat_csr_sparse.data
    #get random walks
    for u in range(num_nodes):
        if len(indices[indptr[u]:indptr[u+1]]) !=0:
            #first move is just depends on weight
            possible_next_node=indices[indptr[u]:indptr[u+1]]
            weight_for_next_move=data[indptr[u]:indptr[u+1]]#i.e  possible next ndoes from u
            weight_for_next_move=weight_for_next_move.astype(np.float32)/np.sum(weight_for_next_move)
            first_walk=np.random.choice(possible_next_node, 1, p=weight_for_next_move)
            random_walk=[u,first_walk[0]]
            for i in range(random_walk_length-2):
                cur_node = random_walk[-1]
                precious_node=random_walk[-2]
                (pi_vx_indices,pi_vx_values)=transition[precious_node,cur_node]
                next_node=np.random.choice(pi_vx_indices, 1, p=pi_vx_values)
                random_walk.append(next_node[0])
            random_walks.append(random_walk)

    return random_walks
"""
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
"""



"""
>> RUN <<
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
"""


# Settings
n_epochs = 10
p = 1.0
q = 1.0
random_walk_length=100
net_folder = '\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/input/'
nets_file = net_folder + 'node_embedding_nets_tmin2_tmin1.csv'
export_folder = '\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/rws/p1q1/'

# Import networks
os.chdir(net_folder)
node_nets = pd.read_csv(nets_file)

# Loop over years and create random walks
unq_years = list(set(node_nets.treatment_year))
loop_iters = list(range(0,len(unq_years)))
for i in loop_iters:
    
    ## Test
    #i=0
    
    # 1) Subset network for current year
    current_net_dt = node_nets[node_nets["treatment_year"] == unq_years[i]]
    
    # 2) Create network(x)
    current_net = networkx.from_pandas_edgelist(current_net_dt,'Workplace_t0','Workplace_t1')
    
    # 2) Create pandas data frame of id-matrix & node-name
    mhm=list(range(0,len(current_net.nodes)))
    year_list = [unq_years[i]] * len(current_net.nodes)
    id_dict = {'node_id':current_net.nodes,
               'sparse_mat_id':mhm,
               'treatment_year':year_list}
    panda_node_id_dict = pd.DataFrame(id_dict)
    
    # 3) Transform to sparse matrix
    current_net_sparse = networkx.to_scipy_sparse_matrix(current_net)
        
    # 4)
    transition = compute_transition_prob(current_net_sparse,p,q)
    # - one epoch
    #random_walks = generate_random_walks(mycr_mat,transition,random_walk_length)    
    #  - multiple epochs
    random_walks = []
    for j in range(0,n_epochs):
        temp = generate_random_walks(current_net_sparse,transition,random_walk_length)
        random_walks = random_walks + temp
        print(j)
    
    # 5) Save random walks as .json
    os.getcwd()
    os.chdir(export_folder)
    random_walks_int = [list(map(int,i)) for i in random_walks]
    current_name = 'rws_' + str(unq_years[i]) + '.json'
    with open(current_name,'w') as f:
        json.dump(random_walks_int,f)
        
    # 6) Save node-id-translation as .csv
    panda_node_id_dict.to_csv(r'id_translation_csvs/' + 'company_to_node_id_' + str(unq_years[i]) + '.csv')
    
    # 7) Print status
    print(i)

