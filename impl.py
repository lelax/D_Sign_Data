from sqlite3 import connect
from pandas import read_csv, Series, read_sql
from csv import reader


class IdentifiableEntity(object):
    def __init__(self, identifiers):
        self.id = set()
        for identifier in identifiers:
            self.id.add(identifier)

    # methods of identifiableEntity

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result

    def addId(self, identifier):
        result = True
        if identifier not in self.id:
            self.id.add(identifier)
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
    def __init__(self, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        fullname = set(familyName + givenName)
        for name in fullname:
            AuthorId =+ 1 #Figure out a way to keep track of the authors ID

    # methods of person
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class Publication(IdentifiableEntity):
    def __init__(self, publicationYear, title, publicationVenue, author):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set(author)

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
        for publicatio in Publication:
            self.id.add(publicatio)

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
    def __init__(self, chapterNumber):
        self.chapterNumber = chapterNumber

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass


class Venue(IdentifiableEntity):
    def __init__(self, title, organization):
        self.title = title
        self.organization = organization

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.organization


class Journal(Venue):
    pass


class Book(Venue):
    pass


class Proceedings(Venue):
    def __init__(self, event):
        self.event = event

    def getEvent(self):
        return self.event


class Organization(IdentifiableEntity):
    def __init__(self, name):
        self.name = name

    def getName(self):
        return self.name


class RelationalDataProcessor(object):

    def __init__(self):
        self.Data = None

    def uploadData(self, Data):
        self.Data = Data
        with open(self.Data, "r", encoding="utf-8") as f:
            Data = reader(f)


class RelationalProcessor(object):

    def __init__(self, dbPath):
        self.dbPath = self.setDbPath  # dbPath: the variable containing the path of the database, initially set as an empty string, that will be updated with the method setDbPath.

    # Methods
    def getDbPath(self):  # it returns the path of the database.
        return self.dbPath

    def setDbPath(self):
        with connect(self.setDbPath) as con:  # it enables to set a new path for the database to handle.
            con.commit()


class GenericQueryProcessor(object):

    with connect ("publications.db") as con:
            con.commit()
    
    with connect ("venue.db") as con:
            con.commit()

    with connect ("journalarticle.db") as con:
            con.commit()

    with connect ("proceedings.db") as con:
            con.commit()

    with connect ("organization.db") as con:
            con.commit()

    def __init__(self, queryProcessor):
        self.queryProcessor = list()


    def cleanQueryProcessors(self):
        self.queryProcessor = self.queryProcessor.clear()


    def addQueryProcessor(self):
        self.queryProcessor.add(self)

    def getPublicationsPublishedInYear(self, year):
            query = "SELECT publicationYear FROM Publication WHERE publicationYear == year"
            df_query = read_sql(query, con)
            return df_query

    def getPublicationsByAuthorId (self, author):
        query = "SELECT publication FROM Publication WHERE AuthorID== author"
        df_query = read_sql(query, con)
        return df_query

    def getMostCitedPublication (self, Cited):

    def getMostCitedVenue (self):

    def getVenuesByPublisherId (self):

    def getPublicationInVenue (self):

    def 
