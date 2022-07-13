from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

store = SPARQLUpdateStore()

endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)):
   store.add(triple)
    
class TriplestoreQueryProcessor(object):

    def getPublicationsPublishedInYear(self, year):
        query= """SELECT ?title

        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:publication_year "2020"  .
        ?publication schema:title ?title  .
    }"""
    
    def getPublicationsByAuthorId(self, author):
        query= """SELECT ?title
    
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:orcid ?"0000-0001-9857-1511"  .
        ?publication schema:title ?title  .
    }"""

    def getMostCitedPublication(self, cites):
        query= """SELECT ?title
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number",
    }
    limit 10"""

    def getMostCitedVenue(self, venues, cites):
        query= """SELECT ?venue
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:cites ?cites  .
            "type": "orderby",
            "variable": "cites_number".
    }
    limit 10"""  

    def getVenuesByPublisherId(self, venues, publishers):
        query= """SELECT ?publication
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: venue_id "issn:0944-1344" .
    }"""

    def getJournalArticlesInIssue(self, issue, volume, identifier):
        query= """SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: issue 9 .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }"""

    def getJournalArticlesInVolume(self, volume, identifier):
        query= """SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: volume 17 .
        ?s schema: identifier "issn:2164-5515" .
    }"""

    def getJournalArticlesInJournal(self, identifier):
        query= """SELECT ?JournalArticle

        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: identifier "issn:2164-5515" .

    }"""

    def getProceedingsByEvent(self, event, name):
        query= """SELECT ?Proceedings

        WHERE {
        ?s rdf:type purl:?Proceedings  .
        ?s name contains(?event,"web")  . 

    }"""

    def getPublicationAuthors(self, author, identifier):
        query= """SELECT ?Author

        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: identifier "doi:10.1080/21645515.2021.1910000" .

    }"""

    def getPublicationsByAuthorName(self, author, name):
        query= """SELECT ?Author

        WHERE {
        ?s rdf:type schema:?Person  .
        ?s schema: filter contains(?name,"doe")  .

    }"""   

    def getDistinctPublisherOfPublications(self,publisher, venue, identifier):
        query= """SELECT ?Publisher

        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?s schema: Venueid ?"doi:10.1080/21645515.2021.1910000" .
        &&
        ?s schema: Venueid ?"doi:10.3390/ijfs9030035" .

    }"""  

#close the connection
store.close()