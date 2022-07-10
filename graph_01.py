from rdflib import Graph

my_graph = Graph()

from rdflib import URIRef

# classes of resources
Person = URIRef("https://schema.org/Person")
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("http://purl.org/spar/fabio/AcademicProceedings")
Venue = URIRef("http://purl.org/dc/elements/1.1/source")
Organization = URIRef("https://schema.org/Organization")

# attributes related to classes
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("http://purl.org/dc/terms/title")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
name = URIRef("https://schema.org/name")
event = URIRef("https://schema.org/Event")
chapterNumber = URIRef("https://github.com/lelax/D_Sign_Data/blob/main/UMLclasses/chapterNumber")
givenName = URIRef ("https://schema.org/givenName")
familyName = URIRef ("https://schema.org/familyName")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
publisher = URIRef ("https://schema.org/publishedBy")
author = URIRef ("http://purl.org/saws/ontology#isWrittenBy")
cites = URIRef ("http://purl.org/spar/cito/isCitedBy")

from rdflib import Literal

a_string = Literal("a string")
a_number = Literal(42)
a_boolean = Literal(True)


from pandas import read_csv, Series
from rdflib import RDF

base_url = "https://github.com/lelax/D_Sign_Data"

publications = read_csv("../D_Sign_Data-1\import\graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                     "id": "string",
                     "title": "string",
                     "type": "string",
                     "publication_year": "string",
                     "issue": "string",
                     "volume": "string",
                     "chapter": "string",
                     "publication_venue": "string",
                     "venue_type": "string",
                     "publisher": "string",
                     "event": "string"
                  })

publication_id = {}
publisher_id = {}
for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)
    
    subj = URIRef(base_url + local_id)
    
    publication_id[row["id"]] = subj
    publisher_id[row["publisher"]] = subj

    
    if row["type"] == "journal-article":
        my_graph.add((subj, RDF.type, JournalArticle))
        # These two statements applies only to journal articles
        my_graph.add((subj, issue, Literal(row["issue"])))
        my_graph.add((subj, volume, Literal(row["volume"])))
        my_graph.add((subj, RDF.type, Journal))
    else:
        my_graph.add((subj, RDF.type, BookChapter))
        #This statement applies only to book chapters
        my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
    
    my_graph.add((subj, title, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["id"])))
    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
    my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
    my_graph.add((subj, Venue, Literal(row["venue_type"])))
    my_graph.add((subj, publisher, Literal(row["publisher"])))
    my_graph.add((subj, event, Literal(row["event"])))

print("-- Number of triples added to the graph after processing the venues")
print(len(my_graph))
