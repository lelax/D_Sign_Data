class Person(object):
    def __init__(self, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

class Publication(object):
    def __init__(self, publicationYear, title, publicationVenue):
        self.publicationYear = publicationYear
        self.title = title


class Venue(object):
    def __init__(self, title, publisher):
        self.title = title

class Organization(object):
    def __init__(self, name):
        self.name = name




