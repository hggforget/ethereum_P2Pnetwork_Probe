import matplotlib.pyplot as plt
import networkx as nx

def buildnet(Nodes,Connections):
    G = nx.Graph()  # 创建空的网络图
    nodes = dict()
    Num = 0
    for i in Nodes:
        Num += 1
        nodes.update({i: Num})
        G.add_node(i)
    for i in Connections:
        if nodes.get(i[0]) and nodes.get(i[1]):
            G.add_edge(i[0], i[1])
    return G
def semi_local_centrality(G):
    N = {}
    Q = {}
    CL = {}
    for node in G.nodes:
        node_nei = list(G.neighbors(node))
        for n_i in node_nei:
            node_nei = node_nei + list(G.neighbors(n_i))
        node_nei = list(set(node_nei))
        N[node] = len(node_nei) - 1

    for node in G.nodes:
        node_nei = list(G.neighbors(node))
        t = 0
        for n_i in node_nei:
            t = t + N[n_i]
        Q[node] = t

    for node in G.nodes:
        node_nei = list(G.neighbors(node))
        t = 0
        for n_i in node_nei:
            t = t + Q[n_i]
        CL[node] = t
    '''
    for node in G.nodes:
        print(node, 'N-value:', N[node], 'Q-value:', Q[node], 'CL-value:', CL[node])
    '''
    return CL
def pltnet(G):
    nx.draw(G,pos = nx.random_layout(G),node_color = 'b',edge_color = 'r',with_labels = True,font_size =0,node_size =20)
    plt.show()
def distinct(G):
    G.remove_nodes_from(list(nx.isolates(G)))
    return G
def net_analyzer(G,isdistinct=1):
    Degrees=nx.degree(G)
    nodes=nx.nodes(G)
    print("节点总数: "+len(nx.nodes(G)).__str__())
    if isdistinct:
        G.remove_nodes_from(list(nx.isolates(G)))
        print("节点总数（去除孤点）: " + len(nodes).__str__())
        C = sorted(nx.connected_components(G), key=len, reverse=True)
        len_list=list()
        for i in C:
            len_list.append(len(i))
            if len(len_list)!=1:
                G.remove_nodes_from(i)
    sum=0
    degrees=list()
    for i in Degrees:
        sum+=i[1]
        if(i[1]>4):
            degrees.append(i[1])
    print("平均度 "+(sum/len(nodes)).__str__())
    nx.degree_centrality(G)
    print("平均最短路径长度: "+nx.average_shortest_path_length(G).__str__())
    #print("degree_centrality: "+nx.degree_centrality(G).__str__())
    print("平均聚集系数: "+nx.average_clustering(G).__str__())
   # print("average_neighbor_degree: "+nx.average_neighbor_degree(G).__str__())
    print("网络直径: " + nx.diameter(G).__str__())
    print("度数大于4的节点数: "+len(degrees).__str__())
    plt.hist(degrees, bins=15)
    plt.xlabel("degrees")
    plt.ylabel("nodes")
    plt.title("degrees distribution")
    plt.show()
