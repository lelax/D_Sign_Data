from unittest import result
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib import URIRef
from pandas import read_csv, Series
from rdflib import RDF
from rdflib import Literal

store = SPARQLUpdateStore()

endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))

    
class TriplestoreQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()

    

# classes of resources
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("http://purl.org/spar/fabio/AcademicProceedings")
Publication = URIRef("https://schema.org/publication")

# attributes related to classes
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("http://purl.org/dc/terms/title")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
name = URIRef("https://schema.org/name")
event = URIRef("https://schema.org/Event")
chapterNumber = URIRef("https://github.com/lelax/D_Sign_Data/blob/main/URIRef/chapterNumber")
givenName = URIRef ("https://schema.org/givenName")
familyName = URIRef ("https://schema.org/familyName")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
publisher = URIRef ("https://schema.org/publishedBy")
author = URIRef ("http://purl.org/saws/ontology#isWrittenBy")
cites = URIRef ("https://schema.org/citation")


a_string = Literal("a string")
a_number = Literal(42)
a_boolean = Literal(True)



publications = read_csv("import/relational_publications.csv",
                    keep_default_na=False,
                    dtype={
                            "doi": "string",
                            "title": "string",
                            "publication year": "int",
                            "issue": "string",
                            "volume": "string",
                            "chapter": "string",
                            "publication venue": "string",
                            "venue_type": "string",
                            "event": "string"
                            })


def getPublicationsPublishedInYear(self, year):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?title

        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:publication_year "2020"  .
        ?publication schema:title ?title  .
    }""")
        return result   

    
def getPublicationsByAuthorId(self, author):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?title
    
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:orcid ?"0000-0001-9857-1511"  .
        ?publication schema:title ?title  .
    }""")
        return result

def getMostCitedPublication(self, cites):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?title
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number",
    }
    limit 10""")
        return result

def getMostCitedVenue(self, cites):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?venue
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number".
    }
    limit 10""")
        return

def getVenuesByPublisherId(self, venue_id):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?publication
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: venue_id "issn:0944-1344" .
    }""")
        return

def getJournalArticlesInIssue(self, issue, volume, identifier):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: issue 9 .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }""")
        return

def getJournalArticlesInVolume(self, volume, identifier):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }""")
        return

def getJournalArticlesInJournal(self, identifier):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?JournalArticle

        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: identifier "issn:2164-5515" .

    }""")
        return

def getProceedingsByEvent(self, event, name):
        store.query("""PREFIX: "http://purl.org/spar/fabio/AcademicProceedings"
        
        SELECT ?Proceedings

        WHERE {
        ?s rdf:type purl:?Proceedings  .
        ?s name contains(?event,"web")  . 

    }""")
        return

def getPublicationAuthors(self, author, identifier):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?Author

        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: identifier "doi:10.1080/21645515.2021.1910000" .

    }""")
        return

def getPublicationsByAuthorName(self, author, name):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?Author

        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: filter contains(?name,"doe")  .

    }""")
        return  

def getDistinctPublisherOfPublications(self,publisher, venue_id, identifier):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?Publisher

        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: Venue_id ?"doi:10.1080/21645515.2021.1910000" .
        &&
        ?s schema: Venue_id ?"doi:10.3390/ijfs9030035" .

    }""")
        return 

#close the connection
        
store.close()
