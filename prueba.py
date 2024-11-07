from difflib import get_close_matches
import pandas as pd
import numpy as np
import os
from datetime import datetime
import unicodedata
import re

df = pd.read_csv("Datasets_Practica_1.2/MantenimientoSucio.csv")

palabra = ','

index = 0

for registro in df.to_dict(orient='records'):
    if index == 399:
        print(type(registro['TIPO']))
        print(registro['TIPO'])
        break
    index += 1
  
