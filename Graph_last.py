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
import json

dsd_graph = Graph()

store = SPARQLUpdateStore()

endpoint = 'http://172.20.10.4:9999/blazegraph/sparql'

store.open((endpoint, endpoint))

for triple in dsd_graph.triples((None, None, None)):
   store.add(triple)

store.close() 


class TriplestoreProcessor(object):
    def __init__(self, endpointUrl=""): # the variable containing the URL of the SPARQL endpoint of the triplestore, initially set as an empty string, that will be updated with the method setEndpointUrl
        self.endpointUrl = endpointUrl
        
    # Methods:
    def getEndpointUrl(self):  # it returns the path of the database
        return self.endpointUrl

    def setEndpointUrl(self, newURL): # it enables to set a new URL for the SPARQL endpoint of the triplestore.
        self.endpointUrl = newURL
        

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None: 
        super().__init__()

    # Method:
def uploadData(self, path): # it enables to upload the collection of data specified in the input file path (either in CSV or JSON) into the database.
    
   
    dsd_graph = Graph()

       

# classes of resources
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("http://purl.org/spar/fabio/AcademicProceedings")
Organization = URIRef("https://schema.org/Organization")
IdentifiableEntity = URIRef("https://schema.org/identifier")
Publication = URIRef("https://schema.org/publication")
Venue = URIRef("https://schema.org/VenueMap")
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
givenName = URIRef("https://schema.org/givenName")
familyName = URIRef("https://schema.org/familyName")
orcid = URIRef("https://schema.org/orcid")
person = URIRef("https://schema.org/person")
venue = URIRef("https://schema.org/venue")
proceedingpapers = URIRef("https://schema.org/proceedingpapers")
doiPublisher = URIRef("https://schema.org/doiPublisher")
doiId = URIRef("https://schema.org/doiId")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
publisher = URIRef("https://schema.org/publishedBy")
author = URIRef("http://purl.org/saws/ontology#isWrittenBy")
citation = URIRef("https://schema.org/citation")

#literal
a_string = Literal("a string ")
a_number = Literal(4)
a_boolean = Literal(True)

base_url = "https://github.com/lelax/D_Sign_Data/"

#Superclass identifiable entity 
IdentifiableEntity = read_csv("graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                })

IdentifiableEntity_internal_id = {}
for idx, row in IdentifiableEntity.iterrows():
    local_id = "IdentifiableEntity-" + str(idx)
    subj = URIRef(base_url + local_id)
    IdentifiableEntity_internal_id[row["id"]] = subj
dsd_graph.add ((subj, identifier, Literal(row["id"])))

if row["type"] == "Person":
    dsd_graph.add((subj, RDF.type, person))
    dsd_graph.add ((subj, givenName, Literal(row["givenName"])))
    dsd_graph.add ((subj, familyName, Literal(row["familyName"])))
elif row["type"] == "Publication":
    dsd_graph.add((subj, RDF.type, Publication))
elif row["type"] == "Venue":
    dsd_graph.add((subj, RDF.type, venue))
elif row["type"] == "Organization":
    dsd_graph.add((subj, RDF.type, Organization))
    dsd_graph.add ((subj, name, Literal(row["name"])))

#Venue
venues = read_csv("graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "title": "string",
                      "type": "string"
                })
venue_internal_id = {}
for idx, row in venues.iterrows():
    local_id = "venue-" + str(idx)
    subj = URIRef(base_url + local_id)
    venue_internal_id[row["id"]] = subj

    if row["type"] == "journal":
        dsd_graph.add((subj, RDF.type, Journal))
    elif row["type"] == "book":
        dsd_graph.add((subj, RDF.type, Book))
    elif row["type"] == "proceeding":
        dsd_graph.add((subj, RDF.type, Proceedings))
        if row["event"] != "":
            dsd_graph.add((subj, event, Literal(row["event"])))
    
    dsd_graph.add((subj, title, Literal(row["title"])))
    dsd_graph.add((subj, identifier, Literal(row["id"])))

#publications
publications = read_csv("graph_publications.csv",
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


publication_internal_id = {}
for idx, row in publications.iterrows():
    local_id = "publication-" + str(idx)

    subj = URIRef(base_url + local_id)

    publication_internal_id[row["id"]] = subj

    dsd_graph.add((subj, doi, Literal(row["id"])))
    dsd_graph.add((subj, title, Literal(row["title"])))
    dsd_graph.add((subj, publicationYear, Literal(row["publication_year"])))

    if row["type"] == "journal-article":
        dsd_graph.add((subj, RDF.type, JournalArticle))

        if row["issue"] != "":
            dsd_graph.add((subj, issue, Literal(row["issue"])))

        if row["volume"] != "":
            dsd_graph.add((subj, volume, Literal(row["volume"])))

    elif row["type"] == "book-chapter":
        dsd_graph.add((subj, RDF.type, BookChapter))

        if row["chapter"] != "":
            dsd_graph.add((subj, chapterNumber, Literal(row["chapter"])))

    elif row["type"] == "proceedings-paper":
        dsd_graph.add((subj, RDF.type, proceedingpapers))

    ven_local_id = "venue-" + str(idx)
    ven_subj = URIRef(base_url + ven_local_id)

    dsd_graph.add((subj, publicationVenue, ven_subj))

#Load json

with open("graph_other_data.json", "r", encoding="utf-8") as f:
    json_doc = json.load(f)

    authors_doi = []
    family_name = []
    given_name = []
    orcid = []

    authors = json_doc['authors']

    for key in authors:
        for item in authors[key]:
            authors_doi.append(key)
            family_name.append(item["family"])
            given_name.append(item["given"])
            orcid.append(item["orcid"])

    # Authors dataframe:
    authors_df = pd.DataFrame({
        "doi": Series(authors_doi, dtype="string", name="doi"),
        "familyName": Series(family_name, dtype="string", name="familyName"),
        "givenName": Series(given_name, dtype="string", name="givenName"),
        "orcid": Series(orcid, dtype="string", name="orcid")
    })

    # Populating the RDF graph with information about the authors

    for idx, row in authors_df.iterrows():
        if authors_df.get(row["orcid"], None) is None:

            local_id = "person-" + str(idx + idx)
            subj = URIRef(base_url + local_id)
            authors[row["orcid"]] = subj

        else:

            subj = author[row["orcid"]]

        dsd_graph.add((subj, RDF.type, person))
        dsd_graph.add((subj, doi, Literal(row["doi"])))
        dsd_graph.add((subj, familyName, Literal(row["familyName"])))
        dsd_graph.add((subj, givenName, Literal(row["givenName"])))
        dsd_graph.add((subj, identifier, Literal(row["orcid"])))

    references = json_doc['references']

    references_doi = []
    references_cites = []

    for key in references:
        for item in references[key]:
            references_doi.append(key)
            references_cites.append(references)

    # References dataframe:
    references_df = pd.DataFrame({
        "references doi": Series(references_doi, dtype="string", name="references doi"),
        "cites": Series(references_cites, dtype="string", name="cites"),
    })

    # Populating the RDF graph with information about the citations
    for idx, row in references_df.iterrows():
        local_id = 'reference-' + str(idx)

        subj = URIRef(base_url + local_id)
        dsd_graph.add((subj, citation, Literal(row['cites'])))

    venues_id = json_doc['venues_id']

    doi = []
    issn_isbn = []

    for key in venues_id:
        for item in venues_id[key]:
            doi.append(key)
            issn_isbn.append(item)

    # Venues dataframe:
    venues_id_df = pd.DataFrame({
        "doi": Series(doi, dtype="string", name="doi"),
        "venues_id": Series(issn_isbn, dtype="string", name="issn_isbn")
    })

    # Populating the RDF graph with information about the venues
    for idx, row in venues_id_df.iterrows():
        local_id = 'venue-' + str(idx)

        subj = URIRef(base_url + local_id)
        dsd_graph.add((subj, venue, Literal(row['venues_id'])))

    publishers = json_doc["publishers"]

    publishers_crossref = []
    publishers_id = []
    publishers_name = []

    for key in publishers:
        for item in publishers[key]:
            publishers_crossref.append(key)
            publishers_id.append(publishers[key]["id"])
            publishers_name.append(publishers[key]["name"])

    # Publishers dataframe:
    publishers_df = pd.DataFrame({
        "crossref": Series(publishers_crossref, dtype="string", name="crossref"),
        "publishers_id": Series(publishers_id, dtype="string", name="publishers_id"),
        "publishers_name": Series(publishers_name, dtype="string", name="publishers_name")
    })

    for idx, row in publishers_df.iterrows():
        local_id = "organization-" + str(idx)
        subj = URIRef(base_url + local_id)

        dsd_graph.add((subj, RDF.type, Organization))
        dsd_graph.add((subj, name, Literal(row["publishers_name"])))
        dsd_graph.add((subj, identifier, Literal(row["publishers_id"])))

        

#print("-- Number of triples added to the graph")
#print(len(dsd_graph))


#LIST OF QUERY BY METHOD

#It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
def getPublicationsPublishedInYear(self, year):
        store.query("""PREFIX schema: <https://schema.org/>
        PREFIX dcterm: <http://purl.org/dc/terms/title>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
        
        SELECT ?title ?type ?publicationYear

        WHERE {
        ?s dcterm:title ?title  .
        ?s rdf:type ?type  .
        ?s schema:publication_year ?year  .
            
    FILTER regex(?publication_year, "{0}")

    }""")

        return result   

    
def getPublicationsByAuthorId(self, author):
        store.query(""" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                        PREFIX fabio: <http://purl.org/spar/fabio/>
                        PREFIX dcterms: <http://purl.org/dc/terms/>
                SELECT ?title
                WHERE {
                ?publication schema:title ?title .
                ?publication fabio:isWrittenBy ?author  .
                ?author purl:orcid ?orcid .

                FILTER regex(?identifier, "{orcid}")
                }""")
        return result

def getMostCitedPublication(self, reference_cites):
        
        store.query(""" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
        
        SELECT ?title ?reference_cites 
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:reference_cites ?reference_cites  .
            "type": "orderby",
            "variable": "cites_number",
    }
    limit 10""")
        return result

def getMostCitedVenue(self, reference_cites):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
        
        SELECT ?venue ?reference_cites
     
        WHERE {
        ?s rdf:type schema:ScholarlyArticle  .
        ?s schema:reference_cites ?reference_cites  .
            "type": "orderby",
            "variable": "cites_number".
    }
    limit 10""")
        return result



def getVenuesByPublisherId(self, publisher_id):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
        
        SELECT ?venue ?publisher
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?publication schema:publishedBy ?publisher .
        ?publisher schema:?doipublisher ?{doi} .

        FILTER regex(?doi = "{crossref:78}")
    }""")
        return result


def getPublicationInVenue(self, venue):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
        
        SELECT ?title ?venue
     
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  .
        ?publication schema:isPartOf ?venue  .
        ?venue schema:identifier ?"{venue_id}"  .
        
        FILTER regex(?venue = "{venue_id}")
    }""")
        return result

#It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input 
# issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input 
# (e.g. "issn:2164-5515").
def getJournalArticlesInIssue(self, issue, volume, identifier):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                
        SELECT ?JournalArticle
    
        WHERE {
        ?publication rdf:type schema:?ScholarlyArticle  ;
           schema:issue ?"{issue}" ;
           schema:volume ?"{volume}" ;
           schema:identifier ?"{id}"  .
           
        FILTER regex(?issue = "{issue}", ?volume = "{volume}", ?identifier ="{id}")
    }""")
        return result

def getJournalArticlesInVolume(self, volume, identifier):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
        
        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:volume ?"{volume}" ;
           schema:identifier ?"{id}"  .
           
        FILTER regex(?volume = "{volume}", ?identifier ="{id}")
    }""")
        return result

def getJournalArticlesInJournal(self, identifier):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>

        SELECT ?JournalArticle
    
        WHERE {
        ?s rdf:type schema:?ScholarlyArticle  ;
           schema:identifier ?"{id}"  .
           
        FILTER regex(?identifier ="{id}")
    }""")
        return result

def getProceedingsByEvent(self, event, event_name):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                        PREFIX purl: <http://purl.org/spar/fabio/AcademicProceedings>
        
        SELECT ?Proceedings

        WHERE {
        ?s purl:type purl:?Proceedings  .
        ?event purl:event_name ?"{event_name}")  . 
        
        FILTER contains(?event_name,"{web}") 
    }""")
        return result

def getPublicationAuthors(self, author, identifier):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                        PREFIX purl: <http://purl.org/saws/ontology#isWrittenBy>
        
        SELECT ?title ?author ?identifier

        WHERE {
        ?s purl:isWrittenBy schema:?author  .
        ?author schema:doi ?"{doi}" .

        FILTER (?doi ="{doi:10.1080/21645515.2021.1910000}")
    }""")
        return result

def getPublicationsByAuthorName(self, author, familyName, givenName):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                        PREFIX purl: <http://purl.org/saws/ontology#isWrittenBy>
                
        SELECT ?title ?author

        WHERE {
        ?s purl:isWrittenBy schema:?author  .
        ?author schema:familyName ?{familyName} 
        &&
        ?author schema:givenName ?{givenName}

        FILTER contains(?familyName, "{doe}" ?givenName, "{doe}")

    }""")
        return result

def getDistinctPublisherOfPublications(self, publisher, venue_id, identifier):
        store.query("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-sintax-ns#>
                        PREFIX schema: <https://schema.org/>
                        PREFIX purl: <http://purl.org/>
                        PREFIX purl: <http://purl.org/saws/ontology#isWrittenBy>
        P               REFIX schema: <https://schema.org/publishedBy">
        
        SELECT ?Publisher ?publication

        WHERE {
        ?venue schema:publishedBy ?identifier  .
        ?identifier schema:venue ?"{venue}".
        &&
        ?identifier schema:venue ?"{venue}" .

        FILTER (?doi = "{doi:10.1080/21645515.2021.1910000}", "{doi:10.3390/ijfs9030035}" )

    }""")
        return result


        






