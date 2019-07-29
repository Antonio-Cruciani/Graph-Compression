
# coding=utf-8
import sys, getopt, os
from pyspark import SparkContext, SparkConf
import math as mt
import bitstring as bs



#--------- Codice
# Group di tipo bytes
# Algoritmo di codifica VarInt GB 
def GB_Encoding(n,group,shift):
    gruppi = bytes()
    cod = bytes()
    #--------- Codifica del Prefisso ( Gruppetto ) ----------
    # Se il numero da codificare è 0 allora basta 1 byte 
    # nel gruppetto avremo 00 
    if (n == 0):
        group = group | (0<< shift)
        shift = shift - 2
        #gruppi = gruppi+bytes([0])
        codificato = bytes(1)
    else:
        # Altrimenti, controlliamo la lunghezza in binario del numero in input
        # e la dividiamo per 8 per capire quanto è lunga la rappresentazione binaria in bytes
        l = mt.floor((mt.log(n,2)+1))/8
        # Se l'estremo superiore della divisione è 1 allora la rappresentazione binaria è lunga 1 byte
        # quindi nel gruppetto metteremo 00 
        if (mt.ceil(l) == 1):
            #metti 00 nel group
            group = group | (0 << shift)
            shift = shift - 2
            #gruppi = gruppi+bytes([0])
        # Se l'estremo superiore della divisione è 2 allora la rappresentazione binaria è lunga 2 byte
        # quindi nel gruppetto metteremo 01 
        if(mt.ceil(l) == 2):
            #metti 01 nel group
            group = group | (1 << shift)
            shift = shift - 2
           # gruppi = gruppi+bytes([1])
        # Se l'estremo superiore della divisione è 3 allora la rappresentazione binaria è lunga 3 byte
        # quindi nel gruppetto metteremo 10
        if(mt.ceil(l) == 3):
            #metti 10 nel group
            group = group | (2 << shift)
            shift = shift - 2
            #gruppi = gruppi+bytes([2])
        # Se l'estremo superiore della divisione è 4 allora la rappresentazione binaria è lunga 4 byte
        # quindi nel gruppetto metteremo 11 
        if(mt.ceil(l) == 4):
            group = group | (3 << shift)
            shift = shift - 2
            #gruppi = gruppi + bytes([3])
        #--------- Fine Codifica del Prefisso ----------
        #--------- Codifica del numero -----------------
        # Codifica del numero in input 
        k = n
        codificato = bytes()
        while(k > 0):
            resto = k % 256
            quoziente = k // 256
            codificato = codificato+bytes([resto])
            k = quoziente
    return codificato,group,shift



# Algoritmo di decodifica VarInt GB 
def GB_Decoding(codificato):
    i=0
    lista = []
    # Ciclo che itera e scorre tutta la codifica (i aumenta di gruppo in gruppo vedi incremento alla fine del while)
    while(i< len(codificato)):
        # Analizziamo il prefisso della codifica
        # first,second,third,foruth sono, rispettivamente: primo,secondo,terzo,quarto elemento del prefisso 
        first = (codificato[i] & (192))>> 6
        second = (codificato[i] & (48))>> 4
        third = (codificato[i] & (12))>> 2
        fourth = (codificato[i] & (3))>> 0  
        # Calcoliamo quanto è lunga la codifica 
        # viene aggiunto +4 poiché sarebbe:
        # somma = first +1 + second +1 + third +1 + fourth +1
        somma = first + second + third + fourth + 4
        # Calcoliamo la lunghezza della codifica senza il prefisso
        lung = (len(codificato[i:(i+somma+1)])-1)
        # Se somma-lung = 0 allora abbiamo che sono stati codificati esattamente 4 numeri e quindi li decodifichiamo
        if (somma-lung == 0):
            #if(i==0):
            l=i+1
            #else:
                #l=i+1
            a = bytes(1)
            # Decodifico il primo
            shift = 0
            #l = i+1
            j=0
            flag = 0
            while(flag == 0):
                if j == 0:
                    a = a[0] | codificato[l]<< shift
                else:
                    a = a | codificato[l]<< shift
                shift += 8
                if j == first:
                    flag = 1
                j+=1
                l+=1
            lista.append(a)
            #decodifico il secondo
            shift = 0 
            flag = 0
            j=0
            b = bytes(1)
            while(flag == 0):
                if j == 0:
                    b = b[0] | codificato[l]<< shift
                else:
                    b = b | codificato[l]<< shift
                shift += 8
                if j == second:
                    flag = 1
                j+=1
                l+=1
            lista.append(b)
            #decodifico il terzo
            shift = 0 
            c = bytes(1)
            flag = 0
            j = 0
            while(flag == 0):
                if j == 0:
                    c = c[0] | codificato[l]<< shift
                else:
                    c = c | codificato[l]<< shift
                shift += 8
                if j == third:
                    flag = 1
                j+=1
                l+=1
            lista.append(c)
            #decodifico il quarto
            shift = 0 
            d = bytes(1)
            flag = 0
            j = 0
            while(flag == 0):
                if j == 0:
                    d = d[0] | codificato[l]<< shift
                else:
                    d = d | codificato[l]<< shift
                shift += 8
                if j == fourth:
                    flag = 1
                j+=1
                l+=1
            lista.append(d)
        # Se somma-lung = 1 allora abbiamo che sono stati codificati esattamente 3 numeri e quindi li decodifichiamo
        elif((somma -lung) == 1 ):
            #if(i==0):
            l=i+1
            #else:
                #l=i+1
            a = bytes(1)
            # Decodifico il primo
            shift = 0
            j=0
            flag = 0
            while(flag == 0):
                if j == 0:
                    a = a[0] | codificato[l]<< shift
                else:
                    a = a | codificato[l]<< shift
                shift += 8
                if j == first:
                    flag = 1
                j+=1
                l+=1
            lista.append(a)
            #decodifico il secondo
            shift = 0 
            flag = 0
            j=0
            b = bytes(1)
            while(flag == 0):
                if j == 0:
                    b = b[0] | codificato[l]<< shift
                else:
                    b = b | codificato[l]<< shift
                shift += 8
                if j == second:
                    flag = 1
                j+=1
                l+=1
            lista.append(b)
            #decodifico il terzo
            shift = 0 
            c = bytes(1)
            flag = 0
            j = 0
            while(flag == 0):
                if j == 0:
                    c = c[0] | codificato[l]<< shift
                else:
                    c = c | codificato[l]<< shift
                shift += 8
                if j == third:
                    flag = 1
                j+=1
                l+=1
            lista.append(c) 
        # Se somma-lung = 2 allora abbiamo che sono stati codificati esattamente 2 numeri e quindi li decodifichiamo
        elif ((somma -lung) == 2 ):
            #if(i==0):
            l=i+1
            #else:
                #l=i+1
            a = bytes(1)
            # Decodifico il primo
            shift = 0
            #l = i+1
            j=0
            flag = 0
            while(flag == 0):
                if j == 0:

                    a = a[0] | codificato[l]<< shift
                else:
                    a = a | codificato[l]<< shift
                shift += 8
                if j == first:
                    flag = 1
                j+=1
                l+=1
            lista.append(a)
            #decodifico il secondo
            shift = 0 
            flag = 0
            j=0
            b = bytes(1)
            while(flag == 0):
                if j == 0:
                    b = b[0] | codificato[l]<< shift
                else:
                    b = b | codificato[l]<< shift
                shift += 8
                if j == second:
                    flag = 1
                j+=1
                l+=1
            lista.append(b)
        # Se somma-lung = 3 allora abbiamo che sono stati codificati esattamente 1 numeri e quindi lo decodifichiamo
        elif ((somma -lung) == 3 ):
            #if(i==0):
            l=i+1
            #else:
            #l=i+1
            a = bytes(1)
            # Decodifico il primo
            shift = 0
            #l = i+1
            j=0
            flag = 0
            while(flag == 0):
                if j == 0:

                    a = a[0] | codificato[l]<< shift
                else:
                    a = a | codificato[l]<< shift
                shift += 8
                if j == first:
                    flag = 1
                j+=1
                l+=1
            lista.append(a)
        # Salto al prefisso successivo che si trova alla posizione: 
        # posizione attuale + first +1 + second +1 + third +1 + fourth +1 +1 = posizione attuale + somma +1 
        i=i+somma+1
    return lista


# Funzione che prende in input una lista di 4 elementi e li codifica in Varint GB
def iter_list(lista):
    codificato = bytes()
    shift = 6
    codificato_app = bytes()
    gruppetto = 0    
    for i in lista:
        codificato_app,gruppetto,shift = GB_Encoding(i,gruppetto,shift)
        codificato = codificato + codificato_app
    codificato = bytes([gruppetto]) + codificato 
    return codificato


# Funzione che codifica una lista di lunghezza variabile in Varint GB
# Praticamente suddivido la lista di lunghezza variabile in liste di lunghezza 4
# e le passo alla funzione iter_list. Chiaramente se la lista ha lunghezza che non è multiplo di 4
# effettuo una passata finale.
def List_Encoding(lista):
    shift = 6
    enc = bytes()
    codificato = bytes()
    codificato_app = bytes()
    gruppetto = 0
    app = []
    j=0
    # Numero di iterazioni che devono essere effettuate 
    # per poter codificare la lista
    # iterations = mt.ceil(len(lista)/4)
    # Praticamente scorro la lista e codifico in Varint GB
    # incremento j fino a 4 e creo i gruppetti
    k = 0
    for i in lista:
        app.append(i)
        j+=1
        if j == 4 :
            enc = enc + iter_list(app)
            app =[]
            j=0
        k+=1
    # Chiaramente se la lunghezza della lista non è un multiplo di 4 c'è bisogno di effettuare 
    # quest ultima passata
    if (len(app)>0):
        enc = enc + iter_list(app)
    return enc


#--Codifica Gamma Code---
# Funzione che ritorna la codifica gammacode di una lista di lunghezza arbitraria
def List_gc_encoding(lista):
    codificato = bs.Bits()
    for i in lista:
        codificato = codificato + elias_gamma_code(i)
    return codificato



# Funzione che ritorna un numero di 0 pari alla lunghezza in binario del numero x (sarebbe il prefisso)
def get_zeros(x):
    a = mt.floor(mt.log(x+1,2))
    return (bs.Bits(a))



# Funzione che calcola la rappresentazione binaria del numero x
def get_bin(x):
    b = bs.Bits()
    while(x>0):
        resto = x%2
        quoziente = x // 2
        b =  bs.Bits([resto])+b
        x = quoziente
    return b



# Funzione che ritorna la codifica Elias Gamma Code del numero n
def elias_gamma_code(n):
    pref = get_zeros(n)
    suff = get_bin(n)
    return pref+suff
#--Fine Gamma Code--


#---- Codifica Golomb 
# Funzione che ritorna la codifica gammacode di una lista di lunghezza arbitraria
def List_gl_encoding(lista,m):
    codificato = bs.Bits()
    for i in lista:
        codificato = codificato + Golomb_encoding(m,i)
    return codificato



# Funzione che permette di verificare se un numero n è una potenza di 2
def check_power_of_two(n):
    
    if n <= 0:
        return False
    else:
        return n & (n - 1) == 0


# Funzione di codifica Golomb        
def Golomb_encoding(m,n):
    q = n//m # quoziente
    r = n%m # resto
    prefix = bs.Bits(q+1)
    if(check_power_of_two(m)):
        binary = get_bin(n)
    else:
        b = mt.ceil(mt.log(m,2))
        if (r<(mt.pow(2,b)-m)):
            binary = get_bin(r)
        else:
            tocode = r+mt.pow(2,b)-m
            binary = get_bin(tocode)
    return prefix + binary
#----- Fine Golomb



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
def get_posting(path,sparkcontext,algorithm = "gb",name=False,gap_opt = False,out_l = False,hits = False,prank = False,hitspagerank = False):
    e_l = sparkcontext.textFile(path)
    e_l = e_l.map(lambda a:(int(a.split("\t")[0]),int(a.split("\t")[1])))
    out_li =''
    # Se è stato scelto di effettuare codifica studiando il grado uscente out_l = True 
    if (out_l):
        e_l =e_l.map(lambda a:(a[1],a[0]))
        out_li = '-out'
    if(hits):
        sname = '-hits-ren'
        # richiama HITS-Alg e fatti ritornare un rdd con (idnodo,hub) 



        # Invertiamo (idnodo,hub) in (hub,idnodo) e fai sortByKey(False)
        rdd_hub_chiavi = hubs.map(lambda x:(x[1],x[0]))
        rdd_hub_chiavi_ordered = rdd_hub_chiavi.sortByKey(False)

        rdd_id_ordinati = rdd_hub_chiavi_ordered.map(lambda x:x[1]).collect()
        k = 0
        dic_old = {}
        dic_new = {}

        for i in rdd_id_ordinati:
            dic_old[k] = i
            dic_new[i] = k
            k+=1

        el_ren = e_l.map(lambda x: (dic_new[x[0]],dic_new[x[1]]))
       
        RDD = el_ren.map(lambda x: (x[0],[x[1]]))

    if(prank):
        sname="-prank-ren"
        # Richiama Pagerank-ALG e fatti ritornare un rdd con (idnodo,pagerank)



        # Invertiamo (idnodo,pagerank) in (pagerank,idnodo) e fai sortByKey(False)

        rdd_pr_chiavi = prank.map(lambda x:(x[1],x[0]))
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


        # Invertiamo (idnodo,pagerank) in (pagerank,idnodo) e fai sortByKey(False)

        rdd_pr_chiavi = prank.map(lambda x:(x[1],x[0]))
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
        sname = ''
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
    f = open("./IR/Progetto/compressi/"+os.path.splitext(os.path.basename(path))[0]+algorithm+out_li+sname+sgap+".enc","wb+")
    if (algorithm =="gb"):
        # File degli offsets
        g = open("./IR/Progetto/compressi/offsets_"+os.path.splitext(os.path.basename(path))[0]+algorithm+out_li+sname+sgap+".txt","w+")
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
            j[1].tofile(f)
    f.close()  
    if (algorithm =="gb"):
        print ("-------- Varint GB Created -----------")
        g.close()
    elif(algorithm == "gc"):
        print ("-------- Gamma code Created -----------")
    


def main(graph_path,d_gap_optimization = False,renamed_optimization = False, out_link = False, algorithm = 'gb',decode = False,find = None,hits = False,prank = False,hitspagerank = False):
    conf = SparkConf().setAppName("miao").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    graph = ''
    gap_opt = False
    out_l = False
    name = False 
    ost = ''
    sname = ''
    sgap = ''
    shits= ''
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
            if(not os.path.exists("./IR/Progetto/compressi/"+os.path.splitext(os.path.basename(graph))[0]+algorithm+ost+sname+sgap+".enc")):
                    print('\n Creazione in corso \n')
                    get_posting(graph,sc,algorithm,name,gap_opt,out_l,shits,sprank,shitsprank)
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


#--------- Se si vuole effettuare la codifica

grafo ='./IR/Progetto/Dataset/enron.edgelist'
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
main(grafo,algorithm = 'gb',renamed_optimization = True )
# Con gap optmization
#main(grafo,algorithm = 'gb',d_gap_optimization = True )
# Con gap opt e frequence opt 
#main(grafo,algorithm = 'gb',d_gap_optimization= True ,renamed_optimization = True )
# ---- Gamma code
# con gammacode senza nessuna ottimizzazione
#main(grafo,algorithm = 'gc') 
#Con freq optimization
#main(grafo,algorithm = 'gc',renamed_optimization = True )
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



#----- Per il decoding (aggiungere eventuali estensioni di ottimizzazione al grafo) ( NOTA DECODING SOLO PER VARINT GB)

#grafo = 'enrongb'
#main(grafo,algorithm = 'gb',decode = True)

#--- Se si vuole effettuare qualche query -- (su enron, eventualmente aggiugnere le estensioni -ren,-gap etc ) (NOTA QUERY SOLO PER VARINT GB)

#grafo = 'enrongb-ren-gap_opt'
#nodo = 1242
#nodo = 39256
#main(grafo,algorithm = 'gb',find = nodo )

#./spark-submit ./IR/Progetto/


