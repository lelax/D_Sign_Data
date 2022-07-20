
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

        from rdflib import Literal

        a_string = Literal("a string")
        a_number = Literal(42)
        a_boolean = Literal(True)


        from pandas import read_csv, Series
        from rdflib import RDF

        base_url = "https://github.com/lelax/D_Sign_Data"

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
                elif:
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
            authors_df = DataFrame({
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
            references_df=DataFrame({
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
            venues_id_df = DataFrame({
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
            publishers_df=DataFrame({
                "crossref": Series(publishers_crossref, dtype="string", name="crossref"),
                "publishers_id": Series(publishers_id, dtype="string", name="publishers_id")
                "publishers_name": Series(publishers_name, dtype="string", name="publishers_name")
            })

            # Populating the RDF graph with information about the venues
            #...
