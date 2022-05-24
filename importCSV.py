import csv
from csv import reader

from sqlalchemy import true

with open("../D_Sign_Data/import/relational_publications.csv", "r", encoding="utf-8") as f:
    publications = reader(f)
    data = csv.reader(f)
    for row in data:
        print(row)

def check(data):
    for i in data:
        if i in 'aeiou':
            return true