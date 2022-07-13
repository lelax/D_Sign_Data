class GenericQueryProcessor():
    qps = []

    def addQueryProcessor(qp):
        qps.append(qp)
        
    def getPublicationPublishedinYear(qp, year):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsPublishedInYear(qp, year))
        
        return result
   
    def getPublicationsByAuthorId(qp, author):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorId(qp, author))
        
        return result
       
    def getMostCitedPublication(qp, cites):
        result = []
        for qp in qps:
            result.append(qp.getMostCitedPublication(qp, cites))
        
        return result
    
       
    def getMostCitedVenue(qp, cites):
        result = []
        for qp in qps:
            result.append(qp.getMostCitedVenue(qp, cites))
        
        return result
    
    def getVenuesByPublisherId(qp, venue_id):
        result = []
        for qp in qps:
            result.append(qp.getVenuesByPublisherId(qp, venue_id))
        
        return result
    
    def getJournalArticlesInIssue(qp, issue, volume, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInIssue(qp, issue, volume, identifier))
        
        return result
    
    def getJournalArticlesInVolume(qp, volume, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInVolume(qp, volume, identifier))
        
        return result

    def getJournalArticlesInJournal(qp, identifier):
        result = []
        for qp in qps:
            result.append(qp.getJournalArticlesInJournal(qp, identifier))
        
        return result
    
    def getProceedingsByEvent(qp, event, name):
        result = []
        for qp in qps:
            result.append(qp.getProceedingsByEvent(qp, event, name))
        
        return result
    
    def getPublicationAuthors(qp, author, identifier):
        result = []
        for qp in qps:
            result.append(qp.getPublicationAuthors(qp, author, identifier))
        
        return result

    def getPublicationsByAuthorName(qp, author, name):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorName(qp, author, name))
        
        return result

    def getDistinctPublisherOfPublication(qp, publisher, venue_id, identifier):
        result = []
        for qp in qps:
            result.append(qp.getPublicationsByAuthorName(qp, publisher, venue_id, identifier))
        
        return result
    
