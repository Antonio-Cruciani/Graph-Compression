import math as mt
import bitstring as bs

# ATTENZIONE !!! QUESTO CODICE VA CAMBIATO

from EncodingAlgorithms.Gamma import get_bin
# ---- Codifica Golomb
# Funzione che ritorna la codifica gammacode di una lista di lunghezza arbitraria
def List_gl_encoding(lista ,m):
    codificato = bytes()
    for i in lista:
        codificato = codificato + Golomb_encoding(m ,i)
    return codificato



# Funzione che permette di verificare se un numero n è una potenza di 2
def check_power_of_two(n):

    if n <= 0:
        return False
    else:
        return n & (n - 1) == 0


# Funzione di codifica Golomb
def Golomb_encoding(m ,n):
    q = n// m  # quoziente
    r = n % m  # resto
    prefix = bs.Bits(q + 1)
    if (check_power_of_two(m)):
        binary = get_bin(n)
    else:
        b = mt.ceil(mt.log(m, 2))
        if (r < (mt.pow(2, b) - m)):
            binary = get_bin(r)
        else:
            tocode = r + mt.pow(2, b) - m
            binary = get_bin(tocode)
    return prefix + binary
# ----- Fine Golomb