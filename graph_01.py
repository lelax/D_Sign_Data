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
chapterNumber = URIRef("http://purl.org/spar/fabio/BookChapter")
givenName = URIRef ("https://schema.org/givenName")
familyName = URIRef ("https://schema.org/familyName")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
publisher = URIRef ("https://schema.org/publishedBy")
author = URIRef ("http://purl.org/saws/ontology#isWrittenBy")
cites = URIRef ("http://purl.org/spar/cito/isCitedBy")

from rdflib import Literal

a_string = Literal("a string")
a_number = Literal(5)
a_boolean = Literal(True)


from pandas import read_csv, Series
from rdflib import RDF

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
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

for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)
    
    # The shape of the new resources that are publications is
    # 'https://comp-data.github.io/res/publication-<integer>'
    subj = URIRef(base_url + local_id)
    
    if row["type"] == "journal-article":
        my_graph.add((subj, RDF.type, JournalArticle))

        # These two statements applies only to journal articles
        my_graph.add((subj, issue, Literal(row["issue"])))
        my_graph.add((subj, volume, Literal(row["volume"])))
    else:
        my_graph.add((subj, RDF.type, BookChapter))
    
    my_graph.add((subj, name, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["doi"])))
    
    # The original value here has been casted to string since the Date type
    # in schema.org ('https://schema.org/Date') is actually a string-like value
    my_graph.add((subj, publicationYear, Literal(str(row["publication year"]))))
    
    # The URL of the related publication venue is taken from the previous
    # dictionary defined when processing the venues
    my_graph.add((subj, publicationVenue, venue_internal_id[row["publication venue"]]))
