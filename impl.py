import pandas as pd
import sqlite3


from sqlite3 import connect
from pandas import read_csv, Series, read_sql
from csv import reader
from pandasql import sqldf



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


class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, publicationVenue, author):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set(author)

        super().__init__(id)

    # methods of publications
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    # not sure about this
    def getCitedPublications(self):
        self.id = set()
        for p in Publication:
            self.id.add(p)

    # missing def getAuthors(self):
    # expected set[Person]




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


class BookChapter(Publication):
    def __init__(self, id, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass


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


class Proceedings(Venue):
    def __init__(self, id, event):
        self.event = event
        super().__init__(id)

    def getEvent(self):
        return self.event


class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class RelationalDataProcessor(object):

    def __init__(self):
            self.uploadData = uploadData

        def read(self, Data):
            if ".json" in Data:
                readjson = read_json(Data)
                print(pd.DataFrame(readjson))
            else:
                readcsv = read_csv(Data)
                print(pd.DataFrame(readcsv))


class RelationalProcessor(object):

    def __init__(self, dbPath):
        self.dbPath = self.setDbPath  # dbPath: the variable containing the path of the database, initially set as an empty string, that will be updated with the method setDbPath.

    # Methods
    def getDbPath(self):  # it returns the path of the database.
        return self.dbPath

    def setDbPath(self):
        with connect(self.setDbPath) as con:  # it enables to set a new path for the database to handle.
            con.commit()


class RelationalQueryProcessor(object):

    def __init__(self):
        self.RelationalQueryProcessor = RelationalQueryProcessor

      
    def getPublicationsPublishedInYear(self, Year):
        query = """SELECT publicationYear 
                 FROM Publication WHERE 
                 publicationYear == Year"""

        publicationYear = read_sql(query, con)


    def getPublicationsByAuthorId(self, Id):
        query = """SELECT publication 
                 FROM Publications 
                 WHERE authorId == Id"""
        pysqldf(query, connect())

    def getMostCitedPublication(self):
        query = """SELECT TOP 1 Title
                   FROM Publication
                   GROUP BY Title
                   ORDER BY COUNT(Title) DESC"""
        pysqldf(query)

    def getMostCitedVenue(self):
        query = """SELECT TOP 1 Title
                   FROM Venue
                   GROUP BY Title
                   ORDER BY COUNT(Title) DESC"""

    def getVenuesByPublisherId(self, id):
        query = """SELECT *
                   FROM Venue
                   WHERE VenueId == OrganizationId"""

    def getPublicationInVenue(self):
        query = """SELECT *
                   FROM Publication
                   WHERE PublicationId == VenueId"""

    def getJournalArticlesInIssue(self, issue, volume, id):
        query = """SELECT * 
                   FROM JournalArticle
                   WHERE id == JournalArticleId
                   issue == JournalArticleIssue
                   volume == JournalArticleVolume"""

    def getJournalArticlesInVolume (self):
        query = """SELECT *
                   FROM JournalArticle
                   WHERE id == JournalArticleId
                   volume == JournalArticleVolume"""
    
    def getJournalArticlesInJournal (self, Id):
        query = """SELECT *
                   FROM JournalArticle
                   WHERE Id == JournalArticleId"""

        journalArticleInJournal = read_sql(query)

    def getProceedingsByEvent (self):
        query = """SELECT *
                   FROM Proceedings
                   WHERE Name in Event"""

    def getPublicationAuthors (self):

    def getPublicationsByAuthorName(self):
    
    def getDistinctPublishersOfPublications(self):



class GenericQueryProcessor(RelationalQueryProcessor):

            def __init__(self, queryProcessor):
                self.queryProcessor = list()
                for l in self.queryProcessor:
                    self.queryProcessor.add(l)

            def cleanQueryProcessors(self):
                self.queryProcessor = self.queryProcessor.clear()

            def addQueryProcessor(self):
                self.queryProcessor.add(self)

            def remove_dotzero(self):
                return self.replace(".0","")

