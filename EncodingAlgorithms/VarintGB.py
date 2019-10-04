
import math as mt

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