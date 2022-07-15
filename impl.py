from multiprocessing import AuthenticationError
import pandas as pd
import sqlite3
from sqlite3 import connect
from pandas import read_csv
from pandas import Series
from pandas import read_sql
from pandas import read_json
from pandas import DataFrame
from csv import reader
from pandas import merge
from json import dump
from csv import reader

#class for Identifiable Entity
class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = set()
        for identifier in id:
            self.id.add(identifier)

    # methods of identifiableEntity

    def getIds(self):
        result = []
        for identifier in self.id:
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

#class for Person
class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        self.fullname = set(self.familyName + self.givenName)
        self.authorInternalId = []
        for i in self.authorInternalId:
            self.authorInternalId.add(i)

        super().__init__(id)

    # methods of person
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName

    def getfullname(self):
        return self.fullname

#class for publication
class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set(author)
        self.cites = cites

        super().__init__(id)

    # methods of publications
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    
    def getCitedPublications(self):
        self.id = set()
        for p in Publication:
            self.id.add(p)

    def getAuthors(self):
        self.author = []
        for p in self.author:
            self.author.add(p)

#class for journal article
class JournalArticle(Publication):
    def __init__(self, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(issue, volume)

    # methods of journal article
    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume

#class for book chapter
class BookChapter(Publication):
    def __init__(self, id, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass

#class for venue
class Venue(IdentifiableEntity):
    def __init__(self, id, title, organization):
        self.title = title
        self.organization = organization
        super().__init__(id)

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.organization


class Journal(Venue):
    pass


class Book(Venue):
    pass

#class for proceedings
class Proceedings(Venue):
    def __init__(self, id, event):
        self.event = event
        super().__init__(id)

    def getEvent(self):
        return self.event

#class for organization
class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name

#classes for the processors
class RelationalProcessor(object):

    def __init__(self, dbPath):
        self.dbPath = "" # dbPath: the variable containing the path of the database, initially set as an empty string, that will be updated with the method setDbPath.

    # Methods
    def getDbPath(self):  # it returns the path of the database.
        return self.dbPath

    def setDbPath(self):
        with connect(self.dbPath) as con:  # it enables to set a new path for the database to handle.
            con.commit()

class RelationalDataProcessor(RelationalProcessor):

    pass
#methods for upload data either in csv or json
    def uploadData(self, Data):
      self.Data = Data
      if ".json" in Data:
        
        readjson = pd.read_json(Data)
        readjson['ID'] = readjson.index #the index containing Ids in the json file will be used in the new "ID" column
        
        authors = readjson["authors"]

        #here we take the existing keys and its values from the authors object from the json file
        #in order to create the authors table
        authorsTable = pd.DataFrame(authors.get([["orcid","family","given"]]))
        
        venuesIdTable = readjson[["venues_id"]]

        referencesTable = readjson[["references"]]

        publishersTable = readjson[["publishers"]]

        with connect(self.getDbPath()) as con:
            authorsTable.to_sql("Authors", con, if_exists="replace", index=False)
            venuesIdTable.to_sql("Venues Id", con, if_exists="replace", index=False)
            referencesTable.to_sql("References", con, if_exists="replace", index=False) 
            publishersTable.to_sql("Publishers", con, if_exists="replace", index=False)
        
        #tables being created and pushed to the database
       
      if ".csv" in Data:
        readcsv = read_csv(Data,
                           keep_default_na=False,
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
        publication_internal_id = []
        readcsv = DataFrame(readcsv)
        publications = DataFrame(readcsv)

        for idx, row in publications.iterrows():

            publication_internal_id.append("publication-"+str(idx))
        
        publications.insert(0, "internalId", Series(publication_internal_id, dtype="string"))

                    
        #Data Frame for Journals
        journals = publications.query("type =='journal'")
        df_joined = merge(journals, publications, left_on="id", right_on="id")
        journals = df_joined["id", "title"]
                
        #Data Frame for Books
        books = publications.query("type == 'book'")
        df_joined = merge(books, publications, left_on="id", right_on="id")
        books = df_joined["id", "title"]
        
        #Data Frame for Proceedings
        proceedings = publications.query("type == 'proceedings'")
        df_joined = merge(proceedings, publications,left_on="id", right_on="id")
        proceedings = df_joined["id", "title", "event"]
        
        #Data Frame for Organization
        organization = publications.query("type == 'organization'")
        df_joined = merge(organization, publications, left_on="id", right_on="id")
        organization = df_joined["id", "title"]
        
        #Data Frame for Journal Articles
        journal_articles = publications.query("type == 'journal article'")
        df_joined = merge(journal_articles, publications, left_on="publication_venue", right_on="id")
        journal_articles = df_joined["id", "publication_year", "issue", "volume"]

        #Data Frame for Event
        event = publications.query("type == 'event'")
        df_joined = merge(event, publications, left_on="id", right_on="id")
        event = df_joined["id", "title"]

        #Data Frame for Book chapter
        book_chapter = publications.query("type == 'book chapter'")
        df_joined = merge(book_chapter, publications, left_on= "publication_venue", right_on="id")
        book_chapter = df_joined["id", "publication_year", "title", "publication_venue"]

        #Data Frame for Proceedings Paper
        proceedings_paper = publications.query("type == 'proceedings paper'")
        df_joined = merge(proceedings_paper, publications,left_on="id", right_on="id")
        proceedings = df_joined["id", "title", "publication_year", "publication_venue"]
        
        #tables being pushed to the database
        with connect(self.getDbPath()) as con:
            publications.to_sql("Publications", con, if_exists="replace", index=False)
            journal_articles.to_sql("JournalArticles", con, if_exists="replace", index=False)
            organization.to_sql("Organization", con, if_exists="replace", index=False)   
            journals.to_sql("Journals", con, if_exists="replace", index=False)
            books.to_sql("Books", con, if_exists="replace", index=False)
            proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
            event.to_sql("Event", con, if_exists="replace", index=False)
            book_chapter.to_sql("Book Chapter", con, if_exists=replace, index=False)
            proceedings_paper.to_sql("Proceedings Paper", con, if_exists="replace", index=False)

       

#for every method in the relational query processor, the connection with the created database is commited first
class RelationalQueryProcessor(RelationalProcessor):

    def __init__(self):
        pass

      
    def getPublicationsPublishedInYear(self, Year):

        self.Year = Year

        with connect (self.getDbPath) as con:
            con.commit()

        queryRel1 = """SELECT * 
                 FROM Publications WHERE 
                 publication_year == ?"""

        r1 = read_sql(queryRel1, con, params=[Year])

        return r1


    def getPublicationsByAuthorId(self, Id):

        self.Id = Id

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel2 = """SELECT * 
                 FROM Publications 
                 LEFT JOIN authorsTable.orcid ON Publications.id == authorsTable.orcid 
                 WHERE orcid == ?"""

        r2 = read_sql(queryRel2, con, params=[Id])

        return r2

    def getMostCitedPublication(self):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel3 = """SELECT TOP 1 title
                   FROM Publications
                   GROUP BY title
                   ORDER BY COUNT(title) DESC"""
        
        r3 = read_sql(queryRel3, con)

        return r3

    def getMostCitedVenue(self):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel4 = """SELECT TOP 1 title
                   FROM venuesIdTable
                   GROUP BY title
                   ORDER BY COUNT(title) DESC"""

        r4 = read_sql(queryRel4, con)

        return r4


    def getVenuesByPublisherId(self, Id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel5 = """SELECT *
                   FROM venuesIdTable
                   WHERE publishersTable.id == ?"""

        r5 = read_sql(queryRel5, con, params=[Id])

        return r5

    def getPublicationInVenue(self, Id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel6 = """SELECT *
                   FROM Publications
                   LEFT JOIN venuesIdTable.id ON Publications.Id == venuesIdTable.id
                   WHERE venuesIdTable.id == ?"""

        r6 = read_sql(queryRel6, con, params=[Id])

        return r6

    def getJournalArticlesInIssue(self, issue, volume, id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel7 = """SELECT * 
                   FROM journal_articles
                   LEFT JOIN venuesIdTable ON journal_articles.id == venuesIdTable.publications.Id 
                   WHERE issue = ? 
                   AND volume = ? 
                   AND venueId = ? """
        
        r7 = read_sql(queryRel7, con, params=[issue, volume, id])

        return r7

    def getJournalArticlesInVolume (self, volume, id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel8 = """SELECT *
                   FROM journal_articles
                   WHERE ? == journal_articles.volume
                   AND
                   ? == journal_articles.id"""

        r8 = read_sql(queryRel8, con, params=[volume, id])

        return r8
    
    def getJournalArticlesInJournal (self, Id):

        with connect(self.getDbPath) as con:
            con.commit()


        queryRel9 = """SELECT *
                   FROM journal_articles
                   WHERE ? == journal_articles.id"""

        r9 = read_sql(queryRel9, con, params=[Id])

        return r9

    def getProceedingsByEvent (self, name):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel10 = """SELECT *
                   FROM Proceedings
                   WHERE ? in Event"""

        r10 = read_sql(queryRel10, con, params=[name])

        return r10

    def getPublicationAuthors (self, Id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel11 = """SELECT *
                        FROM Authors
                        WHERE ? = Authors.id"""

        r11 = read_sql (queryRel11, con, params=[Id])

        return r11

    def getPublicationsByAuthorName(self, name):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel12 = """SELECT *
                        FROM Publications
                        WHERE ? = Authors.given"""

        r12 = read_sql(queryRel12, con, params=[name])

        return r12
    
    def getDistinctPublishersOfPublications(self, Id):

        with connect(self.getDbPath) as con:
            con.commit()

        queryRel13 = """SELECT DISTINCT *
                        FROM Publishers
                        LEFT ON Venues Id.id == Publications.id
                        WHERE ? == Publications.id"""

        r13 = read_sql(queryRel13, con, params=[Id])

        return r13


