import pandas as pd
from pandas import read_csv
from pandas import read_json
from pandas import read_sql
from pandas import merge
from pandas import concat
from pandas import Series
import sqlite3
import json
from sqlite3 import connect


# class for Identifiable Entity
class IdentifiableEntity(object):

    def __init__(self, id):
        self.id = id
        self.id_array = set()
        for identifier in id:
            self.id_array.add(identifier)

    # methods of identifiableEntity

    def getIds(self):
        result = []
        for identifier in self.id_array:
            result.append(identifier)
        result.sort()
        return result

    def addId(self, id):
        result = True
        if id not in self.id:
            self.id.add(id)
        else:
            result = False
        return result

    def removeId(self, identifier):
        result = True
        if identifier in self.id:
            self.id.remove(identifier)
        else:
            result = False
        return result


# class for Person
class Person(IdentifiableEntity):

    def __init__(self, id, givenName, familyName):
        super().__init__(id)
        self.givenName = givenName
        self.familyName = familyName

    # methods of person
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


# class for publication
class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set(author)
        self.cites = set(cites)

        super().__init__(id)

    # methods of publications
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    def getCitedPublications(self):
        self.cites = []
        for p in self.cites:
            self.id.add(p)

        return self.cites

    def getAuthors(self):
        self.author = []
        for p in self.author:
            self.author.add(p)

        return self.author


# class for journal article
class JournalArticle(Publication):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(id, publicationYear, title, publicationVenue, author, cites)

    # methods of journal article
    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume


# class for book chapter
class BookChapter(Publication):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, publicationYear, title, publicationVenue, author, cites)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass


# class for venue
class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title
        self.publisher = set(publisher)
        super().__init__(id)

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher


class Journal(Venue):
    pass


class Book(Venue):
    pass


# class for proceedings
class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event
        super().__init__(id, title, publisher)

    def getEvent(self):
        return self.event


# class for organization
class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class QueryProcessor(object):
    def __init__(self):
        pass


# classes for the processors
class RelationalProcessor(object):

    def __init__(self):
        self.dbPath = ''  # dbPath: the variable containing the path of the database,
        # initially set as an empty string, that will be updated with the method setDbPath.

    # Methods
    def getDbPath(self):  # it returns the path of the database.
        return self.dbPath

    def setDbPath(self, path):
        self.dbPath = path


class RelationalDataProcessor(RelationalProcessor):

    def __init__(self):
        super().__init__()

    def uploadData(self, path): #method for uploading data. Here, in case the file is not a csv nor a json
        self.path = path        #an exception should be raised
        result = True

        while True:
            try:

                if self.path.endswith('csv'):
                    with open(path, "r", encoding="utf-8") as file:
                        publications = pd.read_csv(file, keep_default_na=False,
                                                   dtype={
                                                       "id": "string",
                                                       "title": "string",
                                                       "type": "string",
                                                       "publication_year": "int",
                                                       "issue": "string",
                                                       "volume": "string",
                                                       "chapter": "string",
                                                       "publication_venue": "string",
                                                       "venue_type": "string",
                                                       "publisher": "string",
                                                       "event": "string"
                                                   })
                        #creating empty data frames to be populated with the information coming from csv file
                        journal_article = pd.DataFrame({
                            "internalId", "issue", "volume", "publication_year", "title", "publication_venue", "id"})

                        book_chapter = pd.DataFrame({
                            "internalId", "chapter_number", "publication_year", "title", "publication_venue", "id"})

                        journal = pd.DataFrame({
                            "internalId", "doi", "title", "publisher"})

                        book = pd.DataFrame({
                            "internalId", "doi", "title", "publisher"})

                        proceedings_paper = pd.DataFrame(
                            {'internalId', 'doi', 'title', 'publication_year', 'publication_venue'})

                        proceedings = pd.DataFrame({
                            "internalId", "title", "event", "id", "publisher"})

                        publications = publications.drop_duplicates()

                        pub_ids = pd.DataFrame(publications['id'])
                        #creating internal Ids
                        publications_internal_id = []

                        for idx, row in pub_ids.iterrows():
                            publications_internal_id.append("publications-" + str(idx))

                        publications['internalId'] = pd.Series(publications_internal_id)

                        # Data Frame for publications

                        publications_df = pd.DataFrame(
                            {'internalId', 'doi', 'title', 'publication_year', 'publication_venue', 'publisher'})
                        publications_df['internalId'] = publications['internalId']
                        publications_df['doi'] = publications['id'].astype('str')
                        publications_df['title'] = publications['title'].astype('str')
                        publications_df['type'] = publications['type'].astype('str')
                        publications_df['publication_year'] = publications['publication_year'].astype('int')
                        publications_df['publication_venue'] = publications['publication_venue'].astype('str')
                        publications_df['publisher'] = publications['publisher'].astype('str')

                        # Data Frame for journal article

                        journal_article['internalId'] = publications[publications['type'] == "journal-article"][
                            'internalId'].astype('str')
                        journal_article['doi'] = publications[publications['type'] == "journal-article"]['id'].astype(
                            'str')
                        journal_article['issue'] = publications[publications['type'] == "journal-article"][
                            'issue'].astype('str')
                        journal_article['volume'] = publications[publications['type'] == "journal-article"][
                            'volume'].astype('str')
                        journal_article['publication_year'] = publications[publications['type'] == "journal-article"][
                            'publication_year'].astype('str')
                        journal_article['publication_venue'] = publications[publications['type'] == "journal-article"][
                            'publication_venue'].astype('str')
                        journal_article['title'] = publications[publications['type'] == "journal-article"][
                            'title'].astype('str')

                        # Data Frame for Journal

                        journal['internalId'] = publications[publications['venue_type'] == "journal"][
                            'internalId'].astype('str')
                        journal['doi'] = publications[publications['venue_type'] == "journal"]['id'].astype(
                            'str')
                        journal['title'] = publications[publications['venue_type'] == "journal"][
                            'issue'].astype('str')
                        journal['publisher'] = publications[publications['venue_type'] == "journal"][
                            'volume'].astype('str')

                        # Data Frame for book chapter

                        book_chapter['internalId'] = publications[publications['type'] == "book-chapter"][
                            'internalId'].astype('str')
                        book_chapter['doi'] = publications[publications['type'] == "book-chapter"]['id'].astype('str')
                        book_chapter['chapter'] = publications[publications['type'] == "book-chapter"][
                            'chapter'].astype('str')
                        book_chapter['publication_year'] = publications[publications['type'] == "book-chapter"][
                            'publication_year'].astype('str')
                        book_chapter['publication_venue'] = publications[publications['type'] == "book-chapter"][
                            'publication_venue'].astype('str')
                        book_chapter['title'] = publications[publications['type'] == "book-chapter"][
                            'title'].astype('str')

                        # Data Frame for book

                        book['internalId'] = publications[publications['venue_type'] == "book"][
                            'internalId'].astype('str')
                        book['doi'] = publications[publications['venue_type'] == "book"]['id'].astype(
                            'str')
                        book['title'] = publications[publications['venue_type'] == "book"][
                            'issue'].astype('str')
                        book['publisher'] = publications[publications['venue_type'] == "book"][
                            'volume'].astype('str')

                        # Data Frame for Proceedings

                        proceedings_paper['internalId'] = publications['internalId'].astype('str')
                        proceedings_paper['doi'] = publications['id'].astype('str')
                        proceedings_paper['title'] = publications['title'].astype('str')
                        proceedings_paper['publisher'] = publications['publisher'].astype('str')
                        proceedings_paper['event'] = publications['event'].astype('str')
                        proceedings_paper['publication_venue'] = publications['publication_venue'].astype('str')
                        proceedings_paper['publication_year'] = publications['publication_year'].astype('str')

                        # Data Frame for Proceedings Paper

                        proceedings['internalId'] = publications['internalId'].astype('str')
                        proceedings['doi'] = publications['id'].astype('str')
                        proceedings['title'] = publications['title'].astype('str')
                        proceedings['publisher'] = publications['publisher'].astype('str')
                        proceedings['event'] = publications['event'].astype('str')
                        proceedings['publication_venue'] = publications['publication_venue'].astype('str')

                    with connect(self.dbPath) as con:
                        publications_df.to_sql("Publications", con, if_exists="append", index=False)
                        journal_article.to_sql("JournalArticle", con, if_exists="append", index=False)
                        book_chapter.to_sql("BookChapter", con, if_exists="append", index=False)
                        journal.to_sql("Journal", con, if_exists="append", index=False)
                        book.to_sql("Book", con, if_exists="append", index=False)
                        proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="append", index=False)
                        proceedings.to_sql("Proceedings", con, if_exists="append", index=False)

                    con.commit()

                elif self.path.endswith('.json'):

                    with open(path, "r", encoding="utf-8") as file:
                        venue = json.load(file)

                        # DataFrame for authors being populated
                        authors_df = pd.DataFrame({
                            "doi_authors": pd.Series(dtype="str"),
                            "family": pd.Series(dtype="str"),
                            "given": pd.Series(dtype="str"),
                            "orcid": pd.Series(dtype="str")
                        })

                        family = []
                        given = []
                        orcid = []
                        doi_authors = []

                        authors = venue['authors']
                        for key in authors:
                            for value in authors[key]:
                                doi_authors.append(key)
                                family.append(value['family'])
                                given.append(value['given'])
                                orcid.append(value['orcid'])

                        authors_df['doi_authors'] = doi_authors
                        authors_df['family'] = family
                        authors_df['given'] = given
                        authors_df['orcid'] = orcid
                        authors_df = authors_df.drop_duplicates()

                        # Data Frame for internal ID Venue

                        venues_id_df = pd.DataFrame({
                            "doi_venues_id": pd.Series(dtype="str"),
                            "issn_isbn": pd.Series(dtype="str"),
                        })
                        doi_venues_id = []
                        issn_isbn = []

                        venues_id = venue["venues_id"]
                        for key in venues_id:
                            for value in venues_id[key]:
                                doi_venues_id.append(key)
                                issn_isbn.append(value)

                        venues_id_df["doi_venues_id"] = doi_venues_id
                        venues_id_df["issn_isbn"] = pd.Series(issn_isbn)

                        venues_id_df = venues_id_df.drop_duplicates()

                        venue_int = []
                        for idx, row in venues_id_df.iterrows():
                            venue_int.append("venue-" + str(idx))

                        venues_id_df["internalId"] = venue_int

                        # Data Frame for references
                        references_df = pd.DataFrame({
                            "idCited": pd.Series(dtype="str"),
                            "idCites": pd.Series(dtype="str"),
                            "no": pd.Series(dtype="int64")
                        })

                        id_ref = []
                        id_ref_doi = []

                        references = venue["references"]

                        for key in references:
                            for value in references[key]:
                                id_ref.append(key)
                                id_ref_doi.append(value)

                        references_df["idCited"] = id_ref
                        references_df["idCites"] = id_ref_doi
                        references_df["no"] = references_df.index

                        references_df = references_df.drop_duplicates()

                        # Data Frame for publishers

                        publishers_df = pd.DataFrame({
                            "id_pub": pd.Series(dtype="str"),
                            "name": pd.Series(dtype="str")
                        })

                        doi_pub = []
                        name = []
                        id_pub = []

                        publishers = venue["publishers"]

                        for key in publishers:
                            for value in publishers[key]:
                                doi_pub.append(key)
                                id_pub.append(publishers[key]["id"])
                                name.append(publishers[key]["name"])

                        publishers_df["doi"] = doi_pub
                        publishers_df["id_pub"] = id_pub
                        publishers_df["name"] = name
                        publishers_df["doi_venue"] = venues_id_df["doi_venues_id"]
                        publishers_df = publishers_df.drop_duplicates()

                        publishers_df["internalId"] = venues_id_df['internalId']

                        authors_df["internalId"] = venues_id_df['internalId']

                        references_df["internalId"] = venues_id_df['internalId']

                        # Creating the tables into the database

                    with connect(self.dbPath) as con:

                        authors_df.to_sql("Authors", con, if_exists="append", index=False)
                        venues_id_df.to_sql("Venues", con, if_exists="append", index=False)
                        references_df.to_sql("ReferencesTable", con, if_exists="append", index=False)
                        publishers_df.to_sql("Publishers", con, if_exists="append", index=False)

                    con.commit()

                else:
                    result = False

            except ValueError:
                print("Oops! This doesn't seem a valid file.")
                result = False

            return result


class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):

    def __init__(self):
        super().__init__()
        self.id = None
        self.year = None

    def getPublicationsPublishedInYear(self, year):
        self.year = year

        with connect(self.getDbPath()) as con:
            query = """SELECT Publications.title, Publications.publication_venue,Publications.publication_year, Publications.type,
                            Publishers.name, Authors.orcid, Authors.given, Authors.family, Authors.doi_authors, 
                            JournalArticle.issue, JournalArticle.volume, BookChapter.chapter, ReferencesTable.idCites
                            FROM Publications
                            LEFT JOIN Authors ON Publications.doi == Authors.doi_authors
                            LEFT JOIN ReferencesTable ON Publications.doi == ReferencesTable.idCited
                            LEFT JOIN JournalArticle ON Publications.doi == JournalArticle.doi
                            LEFT JOIN BookChapter ON Publications.doi == BookChapter.doi
                            LEFT JOIN Publishers ON Publishers.doi == Publications.doi
                            WHERE Publications.publication_year ==  '{0}';""".format(year)

            result = read_sql(query, con)

        return result

    def getPublicationsByAuthorId(self, id):
        self.id = id
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
                Publications.publication_venue, Publishers.name, Publications.publication_year, JournalArticle.issue,
                JournalArticle.volume, BookChapter.chapter, Publications.type, ReferencesTable.idCites
                FROM Publications
                LEFT JOIN Authors ON Publications.doi == Authors.doi_authors
                LEFT JOIN JournalArticle ON Publications.doi == JournalArticle.doi
                LEFT JOIN ReferencesTable ON Publications.doi == ReferencesTable.idCited
                LEFT JOIN BookChapter ON Publications.doi == BookChapter.doi
                LEFT JOIN Publishers ON Publishers.doi == Publications.doi
                WHERE Authors.orcid = '{0}'""".format(id)

            result = read_sql(query, con)

        return result

    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
            Publications.publication_venue, Publishers.name, Publications.publication_year, 
            JournalArticle.issue, JournalArticle.volume, BookChapter.chapter, Publications.type
            FROM (SELECT ReferencesTable.idCited, COUNT (ReferencesTable.no) AS Ocurrence
            FROM ReferencesTable
            GROUP BY ReferencesTable.idCited
            ORDER BY Ocurrence DESC
            LIMIT 1)
            LEFT JOIN Publications ON Publications.doi == idCited
            LEFT JOIN JournalArticle ON Publications.doi == JournalArticle.doi
            LEFT JOIN BookChapter ON Publications.doi == BookChapter.doi
            LEFT JOIN Authors ON Publications.doi == Authors.doi_authors
            LEFT JOIN Publishers ON Publishers.doi == Publications.doi"""
            result = read_sql(query, con)

            return result

    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con:
            query = """ 
            SELECT Publications.publication_venue, Venues.doi_venues_id, Publishers.name, Publications.title 
            FROM 
                (SELECT ReferencesTable."idCited", COUNT(ReferencesTable.no) as Ocurrence
                FROM ReferencesTable
                GROUP BY ReferencesTable."idCited"
                ORDER BY Ocurrence DESC
                LIMIT 1)
            LEFT JOIN Publishers ON "idCited" == Publishers.id_pub 
            LEFT JOIN Venues ON "idCited" == Venues.doi_venues_id
            LEFT JOIN Publications ON "idCited" == Publications.doi"""

            result = read_sql(query, con)

    def getVenuesByPublisherId(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT  Publications.title, Venues.doi_venues_id, Publications.publication_venue, 
            Publishers.name, Publishers.id_pub, Publications.publication_year 
            FROM Publications 
            LEFT JOIN Publishers ON Publishers.doi == Publications.doi 
            LEFT JOIN Venues ON Venues.doi_venues_id == Publications.doi WHERE Publishers.doi = '{0}';""".format(id)

            result = read_sql(query, con)

        return result

    def getPublicationInVenue(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
                       Publications.publication_venue, Publications.publication_year, JournalArticle.issue, JournalArticle.volume, 
                       BookChapter.chapter, Publications.type 
                       FROM Publications 
                       LEFT JOIN Authors ON Publications.doi == Authors.doi_authors 
                       LEFT JOIN JournalArticle ON Publications.doi == JournalArticle.doi 
                       LEFT JOIN BookChapter ON Publications.doi == BookChapter.doi 
                       LEFT JOIN Publishers ON Publishers.doi == Publications.doi LEFT JOIN Venues 
                       ON Venues.doi_venues_id == Publications.doi WHERE Venues.doi_venues_id like '{0}';""".format(id)

            result = read_sql(query, con)

        return result

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
                        Publications.publication_venue, JournalArticle.issue, JournalArticle.volume, Publishers.name, 
                        Publications.publication_year 
                        FROM Publications 
                        LEFT JOIN Authors ON Publications.doi == Authors.doi_authors 
                        LEFT JOIN Publishers ON Publishers.doi == Publications.doi 
                        LEFT JOIN Venues ON Venues.doi_venues_id == Publications.doi 
                        LEFT JOIN JournalArticle ON JournalArticle.doi == Publications.doi 
                        WHERE  JournalArticle.issue = '{0}' AND JournalArticle.volume = '{1}' AND Venues.doi_venues_id = '{2}';""".format(
                issue, volume, journalId)

            result = read_sql(query, con)

        return result

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """ SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
                        Publications.publication_venue, JournalArticle.issue, JournalArticle.volume, 
                        Publishers.name, Publications.publication_year
                        FROM Publications
                        LEFT JOIN Authors ON Publications.doi == Authors.doi_authors
                        LEFT JOIN Publishers ON Publishers.doi == Publications.doi
                        LEFT JOIN Venues ON Venues.doi_venues_id == Publications.doi
                        LEFT JOIN JournalArticle ON JournalArticle.doi == Publications.doi
                        WHERE JournalArticle.volume = '{0}' AND Venues.doi_venues_id = '{1}';""".format(volume,
                                                                                                        journalId)

            result = read_sql(query, con)

        return result

    def getJournalArticlesInJournal(self, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
                        Publications.publication_venue, JournalArticle.issue, JournalArticle.volume, Publishers.name, 
                        Publications.publication_year 
                        FROM Publications
                        LEFT JOIN Authors ON Publications.doi == Authors.doi_authors 
                        LEFT JOIN Publishers ON Publishers.doi == Publications.doi 
                        LEFT JOIN Venues ON Venues.doi_venues_id == Publications.doi 
                        LEFT JOIN JournalArticle ON JournalArticle.doi == Publications.doi 
                        WHERE Venues.doi_venues_id = '{0}';""".format(journalId)

            result = read_sql(query, con)

        return result

    def getProceedingsByEvent(self, name):
        with connect(self.getDbPath()) as con:
            query = """SELECT Proceedings.doi, Proceedings.title, Proceedings.publication_venue , Venues.doi_venues_id, Proceedings.publisher, Proceedings.event
            FROM Proceedings 
            LEFT JOIN Venues ON Venues.doi_venues_id == Proceedings.doi
            WHERE Proceedings.event COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' """.format(name)

            result = read_sql(query, con)

        return result

    def getPublicationAuthors(self, publicationId):
        with connect(self.getDbPath()) as con:
            query = """SELECT orcid, given, family
            FROM Authors
            WHERE doi_authors = '{0}';""".format(publicationId)

            result = read_sql(query, con)

        return result

    def getPublicationsByAuthorsName(self, name):
        with connect(self.getDbPath()) as con:
            query = """SELECT Authors.orcid, Authors.given, Authors.family,  Publications.title, Authors.doi_authors, 
            Publications.publication_venue, Publishers.name, Publications.publication_year, JournalArticle.issue, 
            JournalArticle.volume, BookChapter.chapter, Publications.type 
            FROM Publications LEFT JOIN Authors ON 
            Publications.doi == Authors.doi_authors LEFT JOIN JournalArticle ON Publications.doi == 
            JournalArticle.doi LEFT JOIN BookChapter ON Publications.doi == BookChapter.doi LEFT JOIN Publishers ON 
            Publishers.doi == Publications.doi LEFT JOIN Venues ON Venues.doi_venues_id == Publications.doi WHERE 
            family COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR given COLLATE SQL_Latin1_General_CP1_CI_AS 
            LIKE '%{0}%';""".format(name)

            result = read_sql(query, con)

        return result

    def getDistinctPublishersOfPublications(self, pubIdList):
        with connect(self.getDbPath()) as con:
            publisherId = pd.DataFrame()
            for el in pubIdList:
                query = """"SELECT Publishers.doi, Publishers.name, Publishers.doi_venues_id
                            FROM Publishers
                            LEFT JOIN Publications ON Publications.doi == Publishers.doi_venues_id
                            WHERE Publishers.doi_venues_id = '{0}';""".format(el)

                result = read_sql(query, con)
                publisherId = pd.concat([publisherId, result])

        return publisherId


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()

    def cleanQueryProcessors(self):
        self.queryProcessor = self.queryProcessor.clear()
        return True

    def addQueryProcessor(self, qp):
        self.queryProcessor.append(qp)
        return True

    def removeDotZero(self, s):

        if type(s) == int:
            return s.replace(".0", "")
        else:
            return s

    # list of methods

    def getPublicationsPublishedInYear(self, year):
        pub_obj = []
        column_names = ['title', 'publication_venue', 'publication_year', 'type', 'name', 'orcid', 'given', 'family',
                        'doi_authors', 'issue', 'volume', 'chapter', 'idCites']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationsPublishedInYear(year)
            current.columns = ['title', 'publication_venue', 'publication_year', 'type', 'name', 'orcid', 'given',
                               'family', 'doi_authors', 'issue', 'volume', 'chapter', 'idCites']
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)


            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['idCites']:
                cites_l.append(i)

            pub_obj = Publication(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l)

        return pub_obj

    def getPublicationsByAuthorId(self, id):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'name',
                        'publication_year', 'issue', 'volume', 'chapter', 'type', 'idCites']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationsByAuthorId(id)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'name',
                               'publication_year', 'issue', 'volume', 'chapter', 'type', 'idCites']

            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['idCites']:
                cites_l.append(i)

            pub_obj = Publication(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l)

        return pub_obj

    def getMostCitedPublication(self):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'name',
                        'publication_year', 'issue', 'volume', 'chapter', 'type']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getMostCitedPublication()
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'name',
                               'publication_year', 'issue', 'volume', 'chapter', 'type']

            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
        current = current.fillna("NA")
        df_empty = concat([df_empty, current], ignore_index=True)
        df_final = df_empty
        df_final = df_final.values.tolist()

        doi_l = []
        publicationYear_l = []
        title_l = []
        publicationVenue_l = []
        author_l = []
        name_l = []

        for i in df_empty['doi_authors']:
            doi_l.append(i)

        for i in df_empty['publication_year']:
            publicationYear_l.append(i)

        for i in df_empty['title']:
            title_l.append(i)

        for i in df_empty['publication_venue']:
            publicationVenue_l.append(i)

        for i in df_empty['given']:
            author_l.append(i)

        for i in df_empty['name']:
            name_l.append(i)

        pub_obj = Publication(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, name_l)

        return df_final

    def getMostCitedVenue(self):

        column_names = ["publication_venue", "doi_venues_id", "name", "title"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            # print(df)
            current = df.getMostCitedVenue()
            #current.columns = ["publication_venue", "doi_venues_id", "name", "title"]
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna('NA')

        df_final = df_final.values.tolist()

        doi_l = []
        name_l = []
        title_l = []

        for i in df_empty['doi_venues_id']:
            doi_l.append(i)

        for i in df_empty['name']:
            name_l.append(i)

        for i in df_empty['title']:
            title_l.append(i)

        pub_obj = Venue(doi_l, title_l, name_l)

        return pub_obj

    def getVenuesByPublisherId(self, id):
        column_names = ["title", "doi_venues_id", "publication_venue", "name", "id_pub", "publication_year"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getVenuesByPublisherId(id)
            current.columns = ["title", "doi_venues_id", "publication_venue", "name", "id_pub", "publication_year"]
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")

        df_final = df_final.values.tolist()

        doi_l = []
        title_l = []
        pub_l = []

        for i in df_empty['doi_venues_id']:
            doi_l.append(i)

        for i in df_empty['title']:
            title_l.append(i)

        for i in df_empty['name']:
            pub_l.append(i)

        pub_obj = Venue(doi_l, title_l, pub_l)

        return pub_obj

    def getPublicationInVenue(self, id):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'publication_year',
                        'issue', 'volume', 'chapter', 'type']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationInVenue(id)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue',
                               'publication_year',
                               'issue', 'volume', 'chapter', 'type']
            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['doi_authors']:
                cites_l.append(i)

            pub_obj = Publication(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l)

        return pub_obj

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue', 'volume',
                        'name',
                        'publication_year']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getJournalArticlesInIssue(issue, volume, journalId)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue',
                               'volume', 'name',
                               'publication_year']
            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []
            issue_l = []
            volume_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['doi_authors']:
                cites_l.append(i)

            for i in df_empty['issue']:
                issue_l.append(i)

            for i in df_empty['volume']:
                volume_l.append(i)

            pub_obj = JournalArticle(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l, issue_l, volume_l)

        return pub_obj

    def getJournalArticlesInVolume(self, volume, journalId):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue',
                        'volume', 'name', 'publication_year']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getJournalArticlesInVolume(volume, journalId)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue',
                               'volume', 'name', 'publication_year']
            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []
            issue_l = []
            volume_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['doi_authors']:
                cites_l.append(i)

            for i in df_empty['issue']:
                issue_l.append(i)

            for i in df_empty['volume']:
                volume_l.append(i)

            pub_obj = JournalArticle(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l, issue_l, volume_l)

        return pub_obj

    def getJournalArticlesInJournal(self, journalId):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue', 'volume',
                        'name', 'publication_year']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getJournalArticlesInJournal(journalId)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue', 'issue',
                               'volume',
                               'name', 'publication_year']
            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []
            issue_l = []
            volume_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['doi_authors']:
                cites_l.append(i)

            for i in df_empty['issue']:
                issue_l.append(i)

            for i in df_empty['volume']:
                volume_l.append(i)

            pub_obj = JournalArticle(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l, issue_l, volume_l)

        return pub_obj

    def getProceedingsByEvent(self, name):
        column_names = ['doi', 'title', 'publication_venue', 'doi_venues_id', 'publisher', 'event']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getProceedingsByEvent(name)
            current.columns = ['doi', 'title', 'publication_venue', 'doi_venues_id', 'publisher', 'event']
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()

        doi_l = []
        title_l = []
        publisher_l = []
        event_l = []

        for i in df_empty['doi']:
            doi_l.append(i)

        for i in df_empty['title']:
            title_l.append(i)

        for i in df_empty['publisher']:
            publisher_l.append(i)

        for i in df_empty['event']:
            event_l.append(i)

        pub_obj = Proceedings(doi_l, title_l, publisher_l, event_l)

        return pub_obj

    def getPublicationAuthors(self, publicationId):

        column_names = ['orcid', 'given', 'family']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationAuthors(publicationId)
            current.columns = ['orcid', 'given', 'family']
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()

        orcid_l = []
        given_l = []
        family_l = []

        for i in df_empty['orcid']:
            orcid_l.append(i)

        for i in df_empty['given']:
            given_l.append(i)

        for i in df_empty['family']:
            family_l.append(i)

        pub_obj = Person(orcid_l, given_l, family_l)

        return pub_obj

    def getPublicationsByAuthorsName(self, name):
        column_names = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue',
                        'name', 'publication_year', 'issue', 'volume', 'chapter', 'type']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationsByAuthorsName(name)
            current.columns = ['orcid', 'given', 'family', 'title', 'doi_authors', 'publication_venue',
                               'name', 'publication_year', 'issue', 'volume', 'chapter', 'type']
            # print(current.dtypes)
            current["issue"] = current["issue"].astype("string")
            current["volume"] = current["volume"].astype("string")
            current["issue"] = current["issue"].apply(self.removeDotZero)
            current["volume"] = current["volume"].apply(self.removeDotZero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            doi_l = []
            publicationYear_l = []
            title_l = []
            publicationVenue_l = []
            author_l = []
            cites_l = []

            for i in df_empty['doi_authors']:
                doi_l.append(i)

            for i in df_empty['publication_year']:
                publicationYear_l.append(i)

            for i in df_empty['title']:
                title_l.append(i)

            for i in df_empty['publication_venue']:
                publicationVenue_l.append(i)

            for i in df_empty['given']:
                author_l.append(i)

            for i in df_empty['orcid']:
                cites_l.append(i)

            pub_obj = Publication(doi_l, publicationYear_l, title_l, publicationVenue_l, author_l, cites_l)

        return pub_obj

    def getDistinctPublishersOfPublications(self, pubIdList):
        column_names = ['doi', 'name']

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getDistinctPublishersOfPublications(pubIdList)
            current.columns = ['doi', 'name']
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()

        doi_l = []
        name_l = []

        for i in df_empty['doi']:
            doi_l.append(i)

        for i in df_empty['name']:
            name_l.append(i)

        pub_obj = Organization(doi_l, name_l)

        return pub_obj
