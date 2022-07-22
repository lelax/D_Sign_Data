import pandas as pd
from unittest import result
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib import URIRef
from pandas import read_csv, Series
from rdflib import RDF
from rdflib import Literal
from csv import reader
from rdflib import Graph
from rdflib import Literal

class TriplestoreProcessor(object):
    def __init__(self, endpointUrl=""): # the variable containing the URL of the SPARQL endpoint of the triplestore, initially set as an empty string, that will be updated with the method setEndpointUrl
        self.endpointUrl = endpointUrl
        
    # Methods:
    def getEndpointUrl(self):  # it returns the path of the database
        return self.endpointUrl

    def setEndpointUrl(self, newURL): # it enables to set a new URL for the SPARQL endpoint of the triplestore.
        self.endpointUrl = newURL
        

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpointUrl): 
        super().__init__(endpointUrl)

    # Method:
    def uploadData(self, path): # it enables to upload the collection of data specified in the input file path (either in CSV or JSON) into the database.
        self.path = path   
        
      
    
class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self):
        self.queryProcessor = list()

my_graph = Graph()       

        # classes of resources
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceedings = URIRef("http://purl.org/spar/fabio/AcademicProceedings")
        Person = URIRef("https://schema.org/Person")

        # attributes related to classes
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("http://purl.org/dc/terms/title")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        doi = URIRef("https://schema.org/identifier")
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
        citation = URIRef ("https://schema.org/citation")

        
        a_string = Literal("a string")
        a_number = Literal(42)
        a_boolean = Literal(True)


       
        base_url = "https://github.com/lelax/D_Sign_Data"
        path = "https://github.com/lelax/D_Sign_Data/import"
      
        if path.split(".")[1]=='csv':
            publications = read_csv(path, 
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
                                "venue_type": "string",
                                "publisher": "string",
                                "event": "string"
                                })
            # We are adding information about the publications
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
                elif row["type"] == "proceedings-paper": 
                    my_graph.add((subj, RDF.type, ProceedingsPaper))
                #other data
                my_graph.add((subj, title, Literal(row["title"])))
                my_graph.add((subj, publicationYear, Literal(str(row["publication_year"]))))
                my_graph.add((subj, identifier, Literal(str(row["id"]))))

            # We are adding information about the publication venues
            for idx, row in venues.iterrows():
                local_id = "venues-" + str(idx)
                
                subj = URIRef(base_url + local_id)

                venueType = row["venue_type"]
                if venueType["venue_type"] == "journal":
                    my_graph.add((subj, RDF.type, Journal))
                elif venueType["venue_type"] == "book":
                    my_graph.add((subj, RDF.type, BookChapter))
                else:
                    my_graph.add((subj, RDF.type, Proceedings))
                    #This statement applies only to proceedings
                    my_graph.add((subj, event, Literal(row["event"])))

                my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))

        if path.split(".")[1] == 'json': 
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f) 

            authors_doi = []
            family_name = []
            given_name = []
            orcid = []

            for key in authors:
                for item in authors[key]:
                    authors_doi.append(key)
                    family_name.append(item["family"])
                    given_name.append(item["given"])
                    orcid.append(item["orcid"])

            # Authors dataframe:
            authors_df = pd.DataFrame({
                "doi": Series(authors_doi, dtype="string", name="doi"),
                "family name": Series(family_name, dtype="string", name="family name"),
                "given name": Series(given_name, dtype="string", name="given name"),
                "orcid": Series(orcid, dtype="string", name="orcid")
            })

            # Populating the RDF graph with information about the authors
            authors = {}
            for idx, row in authors_df.iterrows():
                if authors.get(row["orcid"], None) == None:

                    local_id = "person-" + str(idx + author_count)
                    subj = URIRef(base_url + local_id)
                    authors[row["orcid"]] = subj

                else:

                    subj = author[row["orcid"]]

                my_graph.add((subj, RDF.type, Person))
                my_graph.add((subj, doi, Literal(row["doi"])))
                my_graph.add((subj, familyName, Literal(row["family name"])))
                my_graph.add((subj, givenName, Literal(row["given name"])))
                my_graph.add((subj, identifier, Literal(row["orcid"])))

            references_doi = []
            references_cites = []
            for key in references:
                for item in references[key]:
                    references_doi.append(key)
                    references_cites.append(references)

            # References dataframe:
            references_df=pd.DataFrame({
                "references doi": Series(references_doi, dtype="string", name="references doi"),
                "cites": Series(references_cites, dtype="string", name="cites"),
            })

            # Populating the RDF graph with information about the citations
            #...

            doi = []
            issn = []

            for key in venues_id:
                for item in venues_id[key]:
                    doi.append(key)
                    issn.append(item)

            # Venues dataframe:
            venues_id_df = pd.DataFrame({
                "doi": Series(doi, dtype="string", name="doi"),
                "venues_id": Series(issn, dtype="string", name="issn")
            })

            # Populating the RDF graph with information about the venues
            #...

            publishers_crossref = []
            publishers_id = []
            publishers_name = []
            for key in publishers:
                for item in publishers[key]:
                    publishers_crossref.append(key)
                    publishers_id.append(item["id"])
                    publishers_name.append(item["name"])

            # Publishers dataframe:
            publishers_df=pd.DataFrame({
                "crossref": Series(publishers_crossref, dtype="string", name="crossref"),
                "publishers_id": Series(publishers_id, dtype="string", name="publishers_id"),
                "publishers_name": Series(publishers_name, dtype="string", name="publishers_name")
            })

            # Populating the RDF graph with information about the venues
            #...

store = SPARQLUpdateStore()

endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

    # It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))
        
for triple in my_graph.triples((None, None, None)):
    store.add(triple)
    store.close()





#SPARQL queries



def getPublicationsPublishedInYear(self, year):
        store.query("""PREFIX schema: ""https://schema.org/"
        
        SELECT ?title

        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:publication_year {year}  .
        ?publication schema:title ?title  .
    
    FILTER (?publication_year = 2020)

    }""")

        return result   

    
def getPublicationsByAuthorId(self, author):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?title
    
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:orcid ?{orcid} .
        ?publication schema:title ?title  .
    
    FILTER (?orcid = "0000-0001-9857-1511")
    
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
        return result



def getVenuesByPublisherId(self, publisher_id):
        store.query("""PREFIX: ""https://schema.org/"
        
        SELECT ?venue
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:publisher ?{id} .

        FILTER (?id = "crossref:78")
    }""")
        return result


def getPublicationInVenue(self, venue):
        store.query("""PREFIX schema: "https://schema.org/"
        
        SELECT ?title
     
        WHERE {
        ?s schema:isPartOf ?{venue}  .
        
        FILTER (?venue = "issn:0944-1344")
    }""")
        return result


def getJournalArticlesInIssue(self, issue, volume, identifier):
        store.query("""PREFIX schema: ""https://schema.org/"
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:issue ?{issue} ;
           schema:volume ?{volume} ;
           schema:identifier ?{identifier}  .
           
        FILTER (?issue = 9, ?volume = 17, ?identifier ="issn:2164-5515")
    }""")
        return result

def getJournalArticlesInVolume(self, volume, identifier):
        store.query("""PREFIX schema: ""https://schema.org/"
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:volume ?{volume} ;
           schema:identifier ?{identifier}  .
           
        FILTER (?volume = 17, ?identifier ="issn:2164-5515")
    }""")
        return result

def getJournalArticlesInJournal(self, identifier):
        store.query("""PREFIX schema: ""https://schema.org/"
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:identifier ?{identifier}  .
           
        FILTER (?identifier ="issn:2164-5515")
    }""")
        return result

def getProceedingsByEvent(self, event, event_name):
        store.query("""PREFIX purl: "http://purl.org/spar/fabio/AcademicProceedings"
        
        SELECT ?Proceedings

        WHERE {
        ?s purl:type purl:?Proceedings  .
        ?event purl:event_name ?{event_name})  . 
        
        FILTER contains(?event_name,"web") 
    }""")
        return result

def getPublicationAuthors(self, author, identifier):
        store.query("""PREFIX purl: "http://purl.org/saws/ontology#isWrittenBy"
        
        SELECT ?title

        WHERE {
        ?s purl:isWrittenBy schema:?Person  .
        ?person schema:doi ?{doi} .

        FILTER (?doi ="doi:10.1080/21645515.2021.1910000")
    }""")
        return result

def getPublicationsByAuthorName(self, author, name):
        store.query("""PREFIX purl: "http://purl.org/saws/ontology#isWrittenBy/"
        
        SELECT ?title

        WHERE {
        ?s purl:isWrittenBy schema:?Person  .
        ?Person schema:name ?{name} .

        FILTER contains(?name,"doe")

    }""")
        return result

def getDistinctPublisherOfPublications(self, publisher, venue_id, identifier):
        store.query("""PREFIX schema: "https://schema.org/publishedBy"
        
        SELECT ?Publisher

        WHERE {
        ?venue schema:publishedBy ?identifier  .
        ?identifier schema:venue ?{venue} .
        &&
        ?identifier schema:venue ?{venue} .

        FILTER (?doi = "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" )

    }""")
        return result


        

