
# coding=utf-8
import sys, getopt, os
from pyspark import SparkContext, SparkConf
import math as mt
import bitstring as bs

from EncodingAlgorithms.VarintGB import iter_list,List_Encoding,GB_Encoding,GB_Decoding
from EncodingAlgorithms.Gamma import elias_gamma_code,get_bin,get_zeros,List_gc_encoding




def query_list(sc,node,graph):
    f = open("./IR/Progetto/compressi/"+graph+".enc","rb")
    nodes = sc.textFile("./IR/Progetto/compressi/offsets_"+graph+".txt")
    last = False
    if ("-ren" in graph):
        nodes = nodes.map(lambda a: (int(a.split(' ')[0]),(int(a.split(' ')[1]),int(a.split(' ')[2]))))
        nodes_ren = nodes.map(lambda a:(a[1][1],a[0]) )
        q = node
        offset_pointer = (nodes.lookup(q)[0])[0]
        offsets_rdd = nodes.map(lambda a: a[1][0])
    else:
        nodes = nodes.map(lambda a:(int(a.split(' ')[0]),int(a.split(' ')[1])))
        q = node
        offset_pointer = nodes.lookup(q)[0]
        offsets_rdd = nodes.map(lambda a: a[1])
    # Calcolo l'offset massimo
    max_offset  = offsets_rdd.reduce(lambda a,b: max (a,b))    
    if(offset_pointer == max_offset):
        last = True
    else: 
        last = False
    app = bytearray()
    if (last):
        f.seek(offset_pointer-1)
        flag = 1
        i = offset_pointer
        while(flag == 1):
            s = f.read(1)
            f.seek(i-1)
            if (not s):
                flag = 0
            else:
                app.extend(f.read(1))
                i+=1
    elif("-gap_opt" in graph):
        app =bytearray()
        nodes_col = nodes.collect()
        lung = len(nodes_col[1:]) +1
        for j in range(0,lung):
            if(nodes_col[j][0] == q):
                l = (nodes_col[j+1][1][0])
                i = nodes_col[j][1][0]
                f.seek(i-1)
                for k in range(i,l):
                    app.extend(f.read(1))
    elif("-ren" in graph):
        i = 1
        app = bytearray()
        nodes_col = nodes.collect()
        lung = len(nodes_col[1:])+1
        for j in range(0,lung):
            if(nodes_col[j][0] == q):
                l = nodes_col[j+1][1][0]
                i = nodes_col[j][1][0]
                f.seek(i-1)
                for k in range(i,l):
                    app.extend(f.read(1))
    else:
        i= 1
        app = bytearray()
        nodes_col = nodes.collect()
        lung = len(nodes_col[1:]) +1
        for j in range(0,lung):
            if(nodes_col[j][0] == q):
                l = nodes_col[j+1][1]
                i = nodes_col[j][1]
                #print ("\n \t I= ",i," l = ",l)
                f.seek(i-1)
                for k in range(i,l):
                    app.extend(f.read(1))
    f.close() 
    decoded_list = GB_Decoding(app)
    if ("-gap_opt" in graph):
        x = [decoded_list[0]]
        x.extend(Get_d_Gap(decoded_list[1:],True))
        decoded_list = x
    if("-ren" in graph):
        lista = []
        for j in decoded_list:
            lista.append(nodes_ren.lookup(j)[0])
        return lista
    else:
        return decoded_list



# Funzione che data una lista la ordina 
def Get_Sorted(n):
    n.sort()
    app = n
    return app


# Funzione che prende in input una lista di elementi in \mathbb{N} e ritorna la lista codificata con metodo
# d-gap oppure che data una lista codificata con d-gap torna la lista originale.
def Get_d_Gap(x,decode = False):
    j = 0
    l = len(x)-1
    a = [x[0]]
    while(j<l):
        if (decode == False):
            a.append(x[j+1]-x[j])
        else: 
            a.append(x[j+1]+a[j])
        j+=1
    return a


def get_names(l,diz):
    return [diz[x] for x in l]


#Funzione di decodifica solo per codifiche senza nessuna ottimizzazione e con ottimizzazione gap
def decode_file(path,sc):
    f = open("./IR/Progetto/compressi/"+path+".enc","rb")
    off = sc.textFile("./IR/Progetto/compressi/offsets_"+path+".txt") 
    off = off.map(lambda a:(int(a.split(' ')[0]),int(a.split(' ')[1])))
    off_list = off.collect()
    offsets_rdd = off.map(lambda a: a[1])
    max_offset = offsets_rdd.reduce(lambda a,b:max(a,b))
    lista = []
    i = 1
    readed = 0
    app = bytearray()
    j = int(off_list[i][1])
    flag = 0
    while(readed != j-1):
        byte = f.read(1)
        app.extend(byte)
        readed += 1
    lista.append(app)
    app = bytearray()
    i += 1
    j = int(off_list[i][1])
    while(byte != b''):
        if (flag == 0):
            if (readed != j-1):
                byte = f.read(1)
                app.extend(byte)
                readed += 1
            else:
                lista.append(app)
                app = bytearray()

                if( j != max_offset):
                    i += 1
                    j = int(off_list[i][1])
                else:
                    flag = 1
        else:
            byte = f.read(1)
            app.extend(byte)
    lista.append(app)
    f.close()
    print (" \n --------- CODIFICA CARICATA ------------")
    posting_list = sc.parallelize(lista)
    posting_list_dec = posting_list.map(lambda a: GB_Decoding(a))
    if("-gap_opt" in path):
        posting_list_dec = posting_list_dec.map(lambda a:(a[0],Get_d_Gap(a[1:],True)))
    else:
        posting_list_dec = posting_list_dec.map(lambda a:(a[0],a[1:]))
    print ("\n ---------- Grafo decompresso -----------\n")
    return posting_list_dec


# Funzione che data una lista di archi ritorna una lista di adiacienza 
def get_posting(path,path2,sparkcontext,algorithm = "gb",name=False,gap_opt = False,out_l = False,hits = False,prank = False,hitspagerank = False):
    e_l = sparkcontext.textFile(path)
    
    e_l = e_l.map(lambda a:(int(a.split("\t")[0]),int(a.split("\t")[1])))
    #print(e_l.collect())
    #print(e_l.collect())
    out_li =''
    sname = ''
    # Se è stato scelto di effettuare codifica studiando il grado uscente out_l = True 
    if (out_l):
        e_l =e_l.map(lambda a:(a[1],a[0]))
        out_li = '-out'
    if(hits):
        sname = '-hits-ren'
        # richiama HITS-Alg e fatti ritornare un rdd con (idnodo,hub) 

        hubs = sparkcontext.textFile(path2)
        hubs = hubs.map(lambda a:(int(a.split(",")[0]),float(a.split(",")[1])))

        # Invertiamo (idnodo,hub) in (hub,idnodo) e fai sortByKey(False)
        rdd_hub_chiavi = hubs.map(lambda x:(x[1],x[0]))

        rdd_hub_chiavi_ordered = rdd_hub_chiavi.sortByKey(False)
       
        rdd_id_ordinati = rdd_hub_chiavi_ordered.map(lambda x:x[1]).collect()
        #print(rdd_id_ordinati)
        k = 0
        dic_old = {}
        dic_new = {}
        # Creiamo il dizionario che ha come chiave il nodo e come valore il suo nuovo id
        # e il secondo dizionario ha come chiave il nuovo id e come valore quello vecchio

        for a in rdd_id_ordinati:
            dic_old[a] = k
            dic_new[k] = a
            k+=1
        #print(e_l.collect())
        el_ren = e_l.map(lambda a:(dic_old[a[0]],dic_old[a[1]]))
        
        RDD = el_ren.map(lambda ab: (ab[0],[ab[1]]))
        
       

    if(prank):
        sname="-prank-ren"
        # Richiama Pagerank-ALG e fatti ritornare un rdd con (idnodo,pagerank)

        p_r = sparkcontext.textFile(path2)
        p_r = p_r.map(lambda a:(int(a.split(",")[0]),float(a.split(",")[1])))


        # Invertiamo (idnodo,pagerank) in (pagerank,idnodo) e fai sortByKey(False)

        rdd_pr_chiavi = p_r.map(lambda x:(x[1],x[0]))
        rdd_pr_chiavi_ordered = rdd_pr_chiavi.sortByKey(False)

        rdd_id_ordinati = rdd_pr_chiavi_ordered.map(lambda x:x[1]).collect()

        k = 0
        dic_old = {}
        dic_new = {}

        for i in rdd_id_ordinati:
            dic_old[k] = i
            dic_new[i] = k
            k+=1

        el_ren = e_l.map(lambda x: (dic_new[x[0]],dic_new[x[1]]))
       
        RDD = el_ren.map(lambda x: (x[0],[x[1]]))



    if(hitspagerank):
        sname = "-hitspagerank-ren"
        # richiama HITS-Alg e fatti ritornare un rdd con (idnodo,hub) 
        # fai partire il pagerank con distribuzione delta di Dirac nello stato associato al nodo con Hubness maggiore
        # calcola il pagerank, ritornalo e agisci come per il pagerank normale
        p_r = sparkcontext.textFile(path2)
        p_r = p_r.map(lambda a:(int(a.split(",")[0]),float(a.split(",")[1])))

        # Invertiamo (idnodo,pagerank) in (pagerank,idnodo) e fai sortByKey(False)

        rdd_pr_chiavi = p_r.map(lambda x:(x[1],x[0]))
        rdd_pr_chiavi_ordered = rdd_pr_chiavi.sortByKey(False)

        rdd_id_ordinati = rdd_pr_chiavi_ordered.map(lambda x:x[1]).collect()

        k = 0
        dic_old = {}
        dic_new = {}

        for i in rdd_id_ordinati:
            dic_old[k] = i
            dic_new[i] = k
            k+=1

        el_ren = e_l.map(lambda x: (dic_new[x[0]],dic_new[x[1]]))
       
        RDD = el_ren.map(lambda x: (x[0],[x[1]]))





    # Se è stato scelto di effettuare la procedura di ottimizzazione che consiste nel 
    # rinominare i nodi in base alla loro frequenza nella lista di adiacienza
    # eg: il nodo v che ha la frequenza massima avrà indice 0 etc.. 
    if(name):
        sname = '-ren'
        # Prendiamo tutti i nodi sorgente
        sources = e_l.map(lambda a:a[0])
        # Identifichiamo i nodi che non hanno in-degree pari a 0
        # Date le sorgenti, eliminiamo tutte quelle con frequenza >= 1
        sinks = sources.subtract(e_l.map(lambda ab: ab[1]))
        # Ora creiamo una coppia (a,0) per ogni nodo che ha grado entrante pari a 0
        sinks_count = sinks.map(lambda a: (a,0))
        # Map che assegna ad ogni nodo target della lista di adiacienza il valore 1
        # inoltre, ora che abbiamo le coppie (a,1), le uniamo con le coppie (a,0)
        RDD_count = e_l.map(lambda ab:(ab[1],1)).union(sinks_count)
        # Per ogni nodo v calcola il grado entrante e creiamo la coppia : 
        # (v,|N_in(v)|) facendo una Reduce su RDD_count. La reduce viene fatta
        # sugli indici. Quindi abbiamo che, dato un generico nodo i, sommiamo quante volte
        # compare come target node.
        RDD_count = RDD_count.reduceByKey(lambda a,b: a+b)
        # Passiamo dalla coppia (v,|N_in(v)|) ---> (|N_in(v)|,v) => ora la chiave è il grado entrante
        RDD_count = RDD_count.map(lambda ab:(ab[1],ab[0]))
        # Ordinamento decrescente dei nodi in base al loro grado entrante 
        RDD_count = RDD_count.sortByKey(False)

        ord_count = RDD_count.map(lambda a:a[1]).collect()
        k = 0
        dic_old = {}
        dic_new = {}
        # Creiamo il dizionario che ha come chiave il nodo e come valore il suo nuovo id
        # e il secondo dizionario ha come chiave il nuovo id e come valore quello vecchio
        for a in ord_count:
            dic_old[a] = k
            dic_new[k] = a
            k+=1
        # Con la seguente map rinominiamo tutti i nodi 
        el_ren = e_l.map(lambda a: (dic_old[a[0]],dic_old[a[1]]))
        RDD = el_ren.map(lambda ab: (ab[0],[ab[1]]))
    else:
        # Altrimenti si prosegue normalmente senza rinominare i nodi 
        #sname = ''
        RDD = e_l.map(lambda ab: (ab[0],[ab[1]]))
    # Reduce sulla chiave: nodo sorgente. E creiamo la lista di adiacenza.
    adj_list = RDD.reduceByKey(lambda a, b: a+b)
    # Ordiniamo la lista di adiacienza di ogni nodo, abbiamo quindi che ogni lista sarà
    # ordinata in modo crescente.
    adj_list = adj_list.map(lambda a:( a[0],Get_Sorted(a[1] ) ) )
    if(gap_opt):
        sgap = '-gap_opt'
        # Utilizziamo una map per creare una lista codificata tramite tecnica d-gap
        adj_gap = adj_list.map(lambda a: ( a[0],Get_d_Gap(a[1],False)))
        lista_finale = []
        for j in adj_gap.collect():
            l = [(j[0],x) for x in j[1]]
            lista_finale.append(l)
        if(name):
            plus = "-ren"
        else:
            plus = ""
        p = open("./IR/gaps/"+os.path.splitext(os.path.basename(path))[0]+sname+sgap+"-GAP"+plus+".csv","w+")
        p.write("id")
        p.write(",")
        p.write("gap")
        p.write("\n")
        for j in lista_finale:
            for i in j:
                p.write(str(i[0]))
                p.write(",")
                p.write(str(i[1]))
                p.write("\n")
        
        p.close()
        return(True)
        if (algorithm =="gb"):
            # Codifichiamo la lista di adiacienza con la codifica VarInt GB
            # Osserviamo che la sorgente non viene codificata. Ci servirà poi per scrivere il file degli offsets
            encoding = adj_gap.map(lambda ab: ( ab[0],List_Encoding([ab[0]]+ab[1]) ) )
        else:
            # Codifichiamo la lista di adiacienza con la codifica Elias Gamma Code
            encoding = adj_gap.map(lambda ab:(ab[0],elias_gamma_code(ab[0])+List_gc_encoding(ab[1])))
    else:
        sgap  =''
        if (algorithm =="gb"):
            # Codifichiamo la lista di adiacienza con la codifica VarInt GB
            encoding = adj_list.map(lambda ab: ( ab[0],List_Encoding([ab[0]]+ab[1])  ) )
        elif(algorithm =="gc"):
            # Codifichiamo la lista di adiacienza con la codifica Elias Gamma Code
            encoding = adj_list.map(lambda ab:(ab[0],elias_gamma_code(ab[0])+List_gc_encoding(ab[1])))
            
    if(name):
        # Se abbiamo rinominato i nodi, allora aggiungiamo all'encoding l'informazione che verrà sfruttata 
        # nell'operazione di decoding del file.
        encoding=encoding.map(lambda a :(a[0],a[1],dic_new[a[0]]))
    f = open("./IR/compressi/"+os.path.splitext(os.path.basename(path))[0]+algorithm+out_li+sname+sgap+".enc","wb+")
    if (algorithm =="gb"):
        # File degli offsets
        g = open("./IR/compressi/offsets_"+os.path.splitext(os.path.basename(path))[0]+algorithm+out_li+sname+sgap+".txt","w+")
    offset = 1
    # Collect che ci restituisce la lista delle coppie in encoding
    encoding_list = encoding.collect()
    # Ciclo che scrive il file di offset e il file della lista di adiacienza codificata
    for j in encoding_list:
        if (algorithm =="gb"):
            if (name):
                g.write(str(j[2])+" "+str(offset)+" "+str(j[0])+"\n")
            else:
                g.write(str(j[0])+" "+str(offset)+"\n")
            w_bytes = f.write(j[1])
            #print (str(j[0])," ", str(offset), " roba = ", j[1], "\n")
            offset = offset+w_bytes
        elif(algorithm == "gc"):
            f.write(j[1])
            #j[1].tofile(f)
    f.close()  
    if (algorithm =="gb"):
        print ("-------- Varint GB Created -----------")
        g.close()
    elif(algorithm == "gc"):
        print ("-------- Gamma code Created -----------")
    return(True)
    


def main(graph_path,path_2,sc,d_gap_optimization = False,renamed_optimization = False, out_link = False, algorithm = 'gb',decode = False,find = None,hits = False,prank = False,hitspagerank = False):
   
    
    graph = ''
    gap_opt = False
    out_l = False
    name = False 
    ost = ''
    sname = ''
    sgap = ''
    shits= False
    sprank = ''
    shitsprank = ''
    if(decode == False):
        if (find == None):
            if(d_gap_optimization):
                sgap ='-d-gap_opt' 
                gap_opt = True
            if(renamed_optimization):
                sname = '-ren_opt'
                name = True
            if(hits):
                sname = '-hits-ren'
                shits = True
            if(prank):
                sname = '-prank-ren'
                sprank = True
            if(hitspagerank):
                sname = '-hitsprank-ren'
                shitsprank = True

            if(out_link):
                ost = '-out'
                out_l = True
            graph = graph_path
            path2= path_2
            if(not os.path.exists("./IR/Progetto/compressi/"+os.path.splitext(os.path.basename(graph))[0]+algorithm+ost+sname+sgap+".enc")):
                    print('\n Creazione in corso \n')
                    result = get_posting(graph,path2,sc,algorithm,name,gap_opt,out_l,shits,sprank,shitsprank)
            else:
                print ("Errore")   
        else:
            graph = graph_path
            sol = query_list(sc,find,graph)
            print ("\n RISPOSTA QUERY = ",sol[1:])
    else:
        graph = graph_path
        print ('\n \t \t ',graph)
        if( (os.path.exists("./IR/Progetto/compressi/"+graph+".enc"))):
            decoded = decode_file(graph,sc)
            print (decoded.collect())
        else: 
            print ("Errore file non trovato")
    return(result)


#--------- Se si vuole effettuare la codifica

#grafo ='./IR/Dataset/uk-2007-05@100000.edgelist'
grafo ='./IR/Dataset/enron.edgelist'
#grafo ='./IR/Dataset/cnr-2000.edgelist'
#path2 = '/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/enron-auths.csv'
path2 = '/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/enron-hubs.csv'
#path2 = '/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/pr_hubs_enron.csv'
#path2 = '/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/cnr-2000-hubs.csv'
#grafo ='./IR/Progetto/Dataset/eu-2005.edgelist'
#grafo = './IR/Progetto/Dataset/dblp-2010-NON_DIRETTO.edgelist'
#grafo =  './IR/Progetto/Dataset/cnr-2000.edgelist'
#grafo = './IR/Progetto/Dataset/amazon-2008-NON_DIRETTO.edgelist'
#grafo = './IR/Progetto/Dataset/wiki_10000'
#grafo = './IR/Progetto/Dataset/uk-2007-05@100000.edgelist'
#grafo = './IR/Progetto/Dataset/in-2004.edgelist'
#grafo = './IR/Progetto/Dataset/uk-2007-05@1000000.edgelist'
#grafo = './IR/Progetto/Dataset/hollywood-2009.edgelist'
# Senza nessuna ottimizzazione e con Varint GB (se si vuole utilizzare Elias Gamma Code impostare algorithm = 'gc')
#main(grafo,algorithm = 'gb')
# Con freq optimization
#main(grafo,path2,algorithm = 'gb',hits = True )
#main(grafo,path2,algorithm = 'gb',d_gap_optimization = True,hits = True )
#main(grafo,path2,algorithm = 'gb',prank = True )
#main(grafo,path2,algorithm = 'gb',d_gap_optimization = True,prank = True)
# Con gap optmization
#main(grafo,algorithm = 'gb' )
# Con gap opt e frequence opt 
#main(grafo,algorithm = 'gb',d_gap_optimization= True ,renamed_optimization = True )
# ---- Gamma code
# con gammacode senza nessuna ottimizzazione
#main(grafo,path2,sc,algorithm = 'gc' ) 
#main(grafo,path2,algorithm = 'gc',hits = True ) 
#Con freq optimization
#main(grafo,path2,algorithm = 'gc', d_gap_optimization = True,hits = True )
# Con gap optmization
#main(grafo,algorithm = 'gc',d_gap_optimization = True )
# Con gap opt e frequence opt 
#main(grafo,algorithm = 'gc',d_gap_optimization= True ,renamed_optimization = True )

#------- Codifica Lista di incidenza

# Senza nessuna ottimizzazione e con Varint GB (se si vuole utilizzare Elias Gamma Code impostare algorithm = 'gc')
#main(grafo,algorithm = 'gb',out_link = True)
# Con freq optimization
#main(grafo,algorithm = 'gb',renamed_optimization = True ,out_link = True)
# Con gap optmization
#main(grafo,algorithm = 'gb',d_gap_optimization = True ,out_link = True)
# Con gap opt e frequence opt 
#main(grafo,algorithm = 'gb',d_gap_optimization= True ,renamed_optimization = True ,out_link = True)
# ---- Gamma code
# con gammacode senza nessuna ottimizzazione
#main(grafo,algorithm = 'gc',out_link = True) 
#Con freq optimization
#main(grafo,algorithm = 'gc',renamed_optimization = True ,out_link = True)
# Con gap optmization
#main(grafo,algorithm = 'gc',d_gap_optimization = True ,out_link = True)
# Con gap opt e frequence opt 
#main(grafo,algorithm = 'gc',d_gap_optimization= True ,renamed_optimization = True ,out_link = True )
conf = SparkConf().setAppName("miao").setMaster("local[*]")
sc = SparkContext(conf = conf)

# Prova nuovo gammacode
main(grafo,path2,sc,algorithm = 'gb',out_link= True,hits = True ) 







#lista_dataset = ['./IR/Dataset/uk-2007-05@100000.edgelist','./IR/Dataset/enron.edgelist','./IR/Dataset/cnr-2000.edgelist','./IR/Dataset/wiki_10000.edgelist']
#lista_hubs = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/uk-2007-05@100000-auths.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/enron-auths.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/cnr-2000-auths.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/wiki_10000-auths.csv']
#lista_pagerank =['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/uk-2007-05@100000-Pagerank.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/pr_hubs_enron.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/pr_hubs_cnr.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/pr_hubs_wiki.csv']
#lista_mixed = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_uk.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_enron.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_cnr.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_wiki.csv']
'''
lista_dataset = ['./IR/Dataset/ljournal.edgelist']
lista_hubs = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/hits_auths_livejournal.csv']
lista_pagerank = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/pr_hubs_livejournal.csv']
lista_mixed = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auths_livejournal.csv']
k=0

for i in lista_dataset:
    
    sol1 = main(i,lista_hubs[k],sc,algorithm = 'gb',d_gap_optimization = True,hits = True )
    sol2 = main(i,lista_pagerank[k],sc,algorithm = 'gb',d_gap_optimization = True,prank = True)
    sol3 = main(i,lista_mixed[k],sc,algorithm = 'gb',d_gap_optimization = True,hitspagerank = True)
    sol4 = main(i,lista_hubs[k],sc,algorithm = 'gb',d_gap_optimization = True )
    k+=1
'''



'''

for i in lista_dataset:
    for j in range(2):
        if(j == 0):
            print("Eseguendo compressione VarintGB su istanza", i,"\n")
            print("con pagerank:",lista_pagerank[k])
            
            sol1=main(i,lista_pagerank[k],sc,algorithm = 'gb',prank = True )
            
            if(sol1):
                print("Eseguendo compressione gammacode su istanza", i,"\n")
                print("con pagerank:",lista_pagerank[k])
                sol2= main(i,lista_pagerank[k],sc,algorithm = 'gc',prank = True )
                
                if(sol2):
                    print("Eseguendo compressione gammacode su istanza", i,"\n")
                    print("con hubs:",lista_hubs[k])
                    sol3= main(i,lista_hubs[k],sc,algorithm = 'gc',hits = True )
                    
            
        else:
            print("Eseguendo compressione gammacode piu' gap su istanza", i,"\n")
            print("con hubs:",lista_hubs[k])
            sol4=main(i,lista_hubs[k],sc,algorithm = 'gc',d_gap_optimization = True,hits = True )
            
            if(sol4):
                print("Eseguendo compressione gammacode piu' gap su istanza", i,"\n")
                print("con pagerank:",lista_pagerank[k])
                sol5=main(i,lista_pagerank[k],sc,algorithm = 'gc',d_gap_optimization = True,prank = True )
                
                if(sol5):
                    print("Eseguendo compressione VarintGB piu' gap su istanza", i,"\n")
                    print("con pagerank:",lista_pagerank[k])
                    sol6=main(i,lista_pagerank[k],sc,algorithm = 'gb',d_gap_optimization = True,prank = True )
                    
           
    k+=1


lista_2= ['./IR/Dataset/enron.edgelist','./IR/Dataset/wiki_10000.edgelist','./IR/Dataset/cnr-2000.edgelist','uk-2007-05@100000.edgelist']
lista_mixed = ['/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_enron.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_wiki.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_cnr.csv','/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_uk.csv']

'''
''''

k =0
for i in lista_2:
        print("Eseguendo compressione VarintGB su istanza", i,"\n")
        print("con pagerank:",lista_mixed[k])
            
        sol1=main(i,lista_mixed[k],sc,algorithm = 'gb',hitspagerank = True )

        if(sol1):
            print("Eseguendo compressione gammacode su istanza", i,"\n")
            print("con pagerank:",lista_mixed[k])
            sol2= main(i,lista_mixed[k],sc,algorithm = 'gb',d_gap_optimization=True,hitspagerank = True )

            if(sol2):
                print("Eseguendo compressione gammacode su istanza", i,"\n")
                print("con pagerank:",lista_mixed[k])
                sol3= main(i,lista_mixed[k],sc,algorithm = 'gc',hitspagerank = True )

                if(sol3):
                    print("Eseguendo compressione gammacode su istanza", i,"\n")
                    print("con pagerank:",lista_mixed[k])
                    sol4= main(i,lista_mixed[k],sc,algorithm = 'gc',d_gap_optimization=True,hitspagerank = True )
        k+=1
    
'''
'''
grafo = './IR/Dataset/enron.edgelist'
auths = '/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/mixed_auths/mixed_auth_enron.csv'
o = main(grafo,auths,sc,algorithm = 'gc',d_gap_optimization=True )
'''
'''
i = './IR/Dataset/uk-2007-05@100000.edgelist'
c ='/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/pr_scores/uk-2007-05@100000-Pagerank.csv'
p ='/Users/antonio/spark-2.3.2-bin-hadoop2.7/bin/IR/hits_scores/uk-2007-05@100000-hubs.csv'
t = main(i,c,algorithm = 'gc',hits = True )
o = main(i,c,algorithm = 'gb',prank = True )
t =  main(i,c,algorithm = 'gc',prank = True )
e = main(i,c,algorithm = 'gc',d_gap_optimization = True,prank = True )
#l = main(i,p,algorithm = 'gc',prank = True )
u = main(i,p,algorithm = 'gb',d_gap_optimization = True,prank = True )
a =main(i,c,algorithm = 'gc',d_gap_optimization = True,prank = True )
#----- Per il decoding (aggiungere eventuali estensioni di ottimizzazione al grafo) ( NOTA DECODING SOLO PER VARINT GB)
'''
#grafo = 'enrongb'
#main(grafo,algorithm = 'gb',decode = True)

#--- Se si vuole effettuare qualche query -- (su enron, eventualmente aggiugnere le estensioni -ren,-gap etc ) (NOTA QUERY SOLO PER VARINT GB)

#grafo = 'enrongb-ren-gap_opt'
#nodo = 1242
#nodo = 39256
#main(grafo,algorithm = 'gb',find = nodo )

#./spark-submit ./IR/Progetto/


