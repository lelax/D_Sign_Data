from sys import orig_argv
from rdflib import Graph

from Classes import Organization, Publication, Venue

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
a_number = Literal(17)
a_boolean = Literal(True)


from pandas import read_csv, Series
from rdflib import RDF

base_url = "https://github.com/lelax/D_Sign_Data"

publications = read_csv("../graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                      "doi": "string",
                      "title": "string",
                      "type": "string",
                    "publication_year": "int",
                    "issue": "string",
                    "volume": "string",
                    "chapter": "int",
                    "publication_venue": "string",
                    "venue_type": "string",
                    "publisher": "string",
                    "event": "string"
                  })

publication_id = {}

for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)

    subj = URIRef(base_url + local_id)
    
    publication_id[row["doi"]] = subj
    
 publication_publisher_id = {}

for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)

    subj = URIRef(base_url + local_id)
    
    publication_publisher_id[row["publisher"]] = subj
    
   if row["type"] == "journal-article":
        my_graph.add((subj, RDF.type, JournalArticle))

        # These two statements applies only to journal articles
        my_graph.add((subj, issue, Literal(row["issue"])))
        my_graph.add((subj, volume, Literal(row["volume"])))
    else:
        my_graph.add((subj, RDF.type, BookChapter))
        #This statement applies only to book chapters
        my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
        
    my_graph.add((subj, title, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["doi"])))
    my_graph.add((subj, publicationYear, Literal(str(row["publication_year"]))))
    my_graph.add((subj, publicationVenue, Literal[row["publication_venue"]]))
    my_graph.add((subj, venue_type, Literal[row["venue_type"]]))
    my_graph.add((subj, publisher, Literal[row["publisher"]]))
    my_graph.add((subj, event, Literal[row["event"]]))
    
