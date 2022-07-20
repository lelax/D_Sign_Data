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

    def addQueryProcessor(self, qp):
        self.queryProcessor.add(qp)
        return True

  #list of methods

    def getPublicationPublishedinYear(self, year):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getPublicationsPublishedInYear(year)
            MyDataFrame = pd.concat([MyDataFrame, result])
           
        return result
              
        

        #the generic query processor will call eather the relational or the triplestore query processor
        #for testing the user will decide the value of the year
   
    def getPublicationsByAuthorId(self, orcid):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
           result = qp.getPublicationsByAuthorId(orcid)
           MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
       
    def getMostCitedPublication(self, cites):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getMostCitedPublication(cites)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
       
    def getMostCitedVenue(self, cites):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getMostCitedVenue(cites)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
    def getVenuesByPublisherId(self, venue_id):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getVenuesByPublisherId(venue_id)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
    def getJournalArticlesInIssue(self, issue, volume, identifier):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getJournalArticlesInIssue(issue, volume, identifier)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
    def getJournalArticlesInVolume(self, volume, identifier):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getJournalArticlesInVolume(volume, identifier)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result

    def getJournalArticlesInJournal(self, identifier):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getJournalArticlesInJournal(identifier)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
    def getProceedingsByEvent(self, event, name):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getProceedingsByEvent(event, name)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
    def getPublicationAuthors(self, author, identifier):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessors:
            result = qp.getPublicationAuthors(author, identifier)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result

    def getPublicationsByAuthorName(self, author, name):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getPublicationsByAuthorName(self, author, name)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result

    def getDistinctPublisherOfPublication(self, publisher, venue_id, identifier):
        MyDataFrame = pd.DataFrame()
        result = []
        for qp in self.queryProcessor:
            result = qp.getPublicationsByAuthorName(publisher, venue_id, identifier)
            MyDataFrame = pd.concat([MyDataFrame, result])
        
        return result
    
