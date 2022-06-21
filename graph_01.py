from sys import orig_argv
from rdflib import Graph

from Classes import Organization, Publication, Venue

my_graph = Graph()

from rdflib import URIRef

# classes of resources
Person = URIRef("https://schema.org/Person")
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("https://schema.org/ScholarlyArticle")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("https://schema.org/Legislation")


# attributes related to classes
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
MostCitedPublication = URIRef()
MostCitedVenues = URIRef()
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
name = URIRef("https://schema.org/name")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

from rdflib import Literal

a_string = Literal("a string")
a_number = Literal(3)
a_boolean = Literal(True)


from pandas import read_csv, Series
from rdflib import RDF

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://github.com/lelax/D_Sign_Data"

venues = read_csv("../graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "name": "string",
                      "type": "string"
                  })

venue_internal_id = {}
for idx, row in venues.iterrows():
    local_id = "venue-" + str(idx)
    

    subj = URIRef(base_url + local_id)
    
    # We put the new venue resources created here, to use them
    # when creating publications
    venue_internal_id[row["id"]] = subj
    
    if row["type"] == "journal":
        # RDF.type is the URIRef already provided by rdflib of the property 
        # 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        my_graph.add((subj, RDF.type, Journal))
    else:
        my_graph.add((subj, RDF.type, Book))
    
    my_graph.add((subj, name, Literal(row["name"])))
    my_graph.add((subj, identifier, Literal(row["id"])))