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
    
