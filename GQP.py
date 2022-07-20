from pandas import DataFrame
from pyparsing import null_debug_action
from impl import RelationalQueryProcessor, RelationalProcessor
from graph_01 import TriplestoreQueryProcessor
import pandas as pd




class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()
        

    def cleanQueryProcessors(self):
        self.queryProcessor = self.queryProcessor.clear()
        return True

    def addQueryProcessor(self):
        self.queryProcessor.add(self)
        return True

  #list of methods

    def getPublicationPublishedinYear(self, year):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getPublicationsPublishedInYear(self, year))
        
        return result
   
    def getPublicationsByAuthorId(self, orcid):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getPublicationsByAuthorId(self, orcid))
        
        return result
       
    def getMostCitedPublication(self, cites):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getMostCitedPublication(self, cites))
        
        return result
    
       
    def getMostCitedVenue(self, cites):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getMostCitedVenue(self, cites))
        
        return result
    
    def getVenuesByPublisherId(self, venue_id):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getVenuesByPublisherId(self, venue_id))
        
        return result
    
    def getJournalArticlesInIssue(self, issue, volume, identifier):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getJournalArticlesInIssue(self, issue, volume, identifier))
        
        return result
    
    def getJournalArticlesInVolume(self, volume, identifier):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getJournalArticlesInVolume(self, volume, identifier))
        
        return result

    def getJournalArticlesInJournal(self, identifier):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getJournalArticlesInJournal(self, identifier))
        
        return result
    
    def getProceedingsByEvent(self, event, name):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getProceedingsByEvent(self, event, name))
        
        return result
    
    def getPublicationAuthors(self, author, identifier):
        result = []
        for qp in self.queryProcessors:
            result.append(qp.getPublicationAuthors(self, qp, author, identifier))
        
        return result

    def getPublicationsByAuthorName(self, author, name):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getPublicationsByAuthorName(self, author, name))
        
        return result

    def getDistinctPublisherOfPublication(self, publisher, venue_id, identifier):
        result = []
        for qp in self.queryProcessor:
            result.append(qp.getPublicationsByAuthorName(self, publisher, venue_id, identifier))
        
        return result
    
