import csv
from csv import reader

with open("../D_Sign_Data/import/relational_publications.csv", "r", encoding="utf-8") as f:
    publications = reader(f)
    data = csv.reader(f)
    for row in data:
        print(row)