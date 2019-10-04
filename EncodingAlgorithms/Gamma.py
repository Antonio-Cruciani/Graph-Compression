import math as mt
# ATTENZIONE!!!! QUESTO CODICE VA CAMBIATO 
# Funzione che ritorna la codifica Elias Gamma Code del numero n
def elias_gamma_code(n):
    pref = get_zeros(n)
    suff = get_bin(n)
    return pref+suff


# Funzione che calcola la rappresentazione binaria del numero x
def get_bin(x):
    b = bytes()
    while(x>0):
        resto = x%2
        quoziente = x // 2
        b =  b + bytes([resto])
        x = quoziente
    return b



# Funzione che ritorna la codifica gammacode di una lista di lunghezza arbitraria
def List_gc_encoding(lista):
    codificato = bytes()
    for i in lista:
        codificato = codificato + elias_gamma_code(i)
    return codificato



# Funzione che ritorna un numero di 0 pari alla lunghezza in binario del numero x (sarebbe il prefisso)
def get_zeros(x):
    # Lunghezza in binario del numero
    a = mt.floor(mt.log(x+1,2))
    # Calcolo il numero di byte necessari per rappresentare tale codifica
    l = a / 8
    lunghezza = mt.ceil(l)
    # Ritorno un prefisso
    return (bytes(lunghezza))




