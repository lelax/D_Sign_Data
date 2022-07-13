class GenericQueryProcessor():
    qps = []

    def addQueryProcessor(qp):
        qps.append(qp)

    def getPublicationPublishedinYear(self, qp, year):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsPublishedInYear(self,qp, year))
        
        return result
   
    def getPublicationsByAuthorId(self, qp, author):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorId(self, qp, author))
        
        return result
       
    def getMostCitedPublication(self, qp, cites):
        result = []
        for qp in qps:
            result.append(qp.getMostCitedPublication(self, qp, cites))
        
        return result
    
       
    def getMostCitedVenue(self, qp, cites):
        result = []
        for qp in qps:
            result.append(qp.getMostCitedVenue(self, qp, cites))
        
        return result
    
    def getVenuesByPublisherId(self, qp, venue_id):
        result = []
        for qp in qps:
            result.append(qp.getVenuesByPublisherId(self, qp, venue_id))
        
        return result
    
    def getJournalArticlesInIssue(self, qp, issue, volume, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInIssue(self, qp, issue, volume, identifier))
        
        return result
    
    def getJournalArticlesInVolume(self, qp, volume, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInVolume(self, qp, volume, identifier))
        
        return result

    def getJournalArticlesInJournal(self, qp, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInJournal(self, qp, identifier))
        
        return result
    
    def getProceedingsByEvent(self, qp, event, name):
        result = []
        for qp in qps:
            result.append(qp.getProceedingsByEvent(self, qp, event, name))
        
        return result
    
    def getPublicationAuthors(self, qp, author, identifier):
        result = []
        for qp in qps:
            result.append(qp.getPublicationAuthors(self, qp, author, identifier))
        
        return result

    def getPublicationsByAuthorName(self, qp, author, name):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorName(self, qp, author, name))
        
        return result

    def getDistinctPublisherOfPublication(self, qp, publisher, venue_id, identifier):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorName(self, qp, publisher, venue_id, identifier))
        
        return result
    
