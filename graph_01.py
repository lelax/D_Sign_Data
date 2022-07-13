from csv import reader

class TriplestoreProcessor(object):
    def __init__(self, endpointUrl=""): # the variable containing the URL of the SPARQL endpoint of the triplestore, initially set as an empty string, that will be updated with the method setEndpointUrl
        self.endpointUrl = endpointUrl
        
    # Methods:
    def getEndpointUrl(self):  # it returns the path of the database
        return self.endpointUrl

    def setEndpointUrl(self, newURL): # it enables to set a new URL for the SPARQL endpoint of the triplestore.
        self.endpointUrl = newURL
        

class TriplestoreDataProcessor(object):
    def __init__(self):
        self.Data = None

    # Method:
    def uploadData(self, Data): # it enables to upload the collection of data specified in the input file path (either in CSV or JSON) into the database.
        self.Data = Data
        with open(self.Data, "r", encoding="utf-8") as f:
            Data = reader(f)

from rdflib import Graph

my_graph = Graph()

from rdflib import URIRef

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
                     "publication_year": "int",
                     "publication_venue": "string",
                     "issue": "string",
                     "volume": "string",
                     "chapter": "string",
                  })

for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)
    
    subj = URIRef(base_url + local_id)

    if row["type"] == "journal-article":
        my_graph.add((subj, RDF.type, JournalArticle))
        # These two statements applies only to journal articles
        my_graph.add((subj, issue, Literal(row["issue"])))
        my_graph.add((subj, volume, Literal(row["volume"])))
    elif row["type"] == "book-chapter":
        my_graph.add((subj, RDF.type, BookChapter))
        #This statement applies only to book chapters
        my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
    else: 
        my_graph.add((subj, RDF.type, ProceedingsPaper))

    my_graph.add((subj, cites, Publication))
    my_graph.add((subj, title, Literal(row["title"])))
    my_graph.add((subj, publicationYear, Literal(str(row["publication_year"]))))
    my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
    my_graph.add((subj, identifier, Literal(str(row["id"]))))



venues = read_csv("../D_Sign_Data-1\import\graph_publications.csv", 
                 keep_default_na=False,
                 dtype={
                     "venue_type": "string",
                     "publisher": "string",
                     "event": "string"
                  })

for idx, row in venues.iterrows():
    local_id = "venues-" + str(idx)
    
    subj = URIRef(base_url + local_id)

    if row["venue_type"] == "journal":
        my_graph.add((subj, RDF.type, Journal))
    elif row["venue_type"] == "book":
        my_graph.add((subj, RDF.type, BookChapter))
    else:
        my_graph.add((subj, RDF.type, Proceedings))
        #This statement applies only to proceedings
        my_graph.add((subj, event, Literal(row["event"])))

    my_graph.add((subj, publisher, Literal(row["publisher"])))

 
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
    
