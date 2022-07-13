from sys import orig_argv
from rdflib import Graph

from impl import Organization, Publication, Venue

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

import pandas as pd
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
 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

store = SPARQLUpdateStore()

endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))
    
class TriplestoreQueryProcessor(object):

    def getPublicationsPublishedInYear(self, year):
        store.query("""SELECT ?title
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:publication_year "2020"  .
        ?publication schema:title ?title  .
    }""")
        return    

    
    def getPublicationsByAuthorId(self, author):
        store.query("""SELECT ?title
    
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:orcid ?"0000-0001-9857-1511"  .
        ?publication schema:title ?title  .
    }""")
        return

    def getMostCitedPublication(self, cites):
        store.query("""SELECT ?title
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number",
    }
    limit 10""")
        return

    def getMostCitedVenue(self, venues, cites):
        store.query("""SELECT ?venue
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number".
    }
    limit 10""")
        return

    def getVenuesByPublisherId(self, venues, publishers):
        store.query("""SELECT ?publication
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: venue_id "issn:0944-1344" .
    }""")
        return

    def getJournalArticlesInIssue(self, issue, volume, identifier):
        store.query("""SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: issue 9 .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }""")
        return

    def getJournalArticlesInVolume(self, volume, identifier):
        store.query("""SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }""")
        return

    def getJournalArticlesInJournal(self, identifier):
        store.query("""SELECT ?JournalArticle
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: identifier "issn:2164-5515" .
    }""")
        return

    def getProceedingsByEvent(self, event, name):
        store.query("""SELECT ?Proceedings
        WHERE {
        ?s rdf:type purl:?Proceedings  .
        ?s name contains(?event,"web")  . 
    }""")
        return

    def getPublicationAuthors(self, author, identifier):
        store.query("""SELECT ?Author
        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: identifier "doi:10.1080/21645515.2021.1910000" .
    }""")
        return

    def getPublicationsByAuthorName(self, author, name):
        store.query("""SELECT ?Author
        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: filter contains(?name,"doe")  .
    }""")
        return  

    def getDistinctPublisherOfPublications(self,publisher, venue, identifier):
        store.query("""SELECT ?Publisher
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: Venueid ?"doi:10.1080/21645515.2021.1910000" .
        &&
        ?s schema: Venueid ?"doi:10.3390/ijfs9030035" .
    }""")
        return 

#close the connection
store.close()
    
