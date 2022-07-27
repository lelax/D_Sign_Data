from impl import RelationalDataProcessor, RelationalQueryProcessor, GenericQueryProcessor

rel_path = "relational3.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
print(rel_dp.uploadData("relational_publications.csv")) #If the files are located in the same folder,
print(rel_dp.uploadData("relational_other_data.json")) #that's how they should be uploaded


rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)

result_q1 = generic.getPublicationsPublishedInYear(2020)
result_q2 = generic.getPublicationsByAuthorId('0000-0001-5506-523X')
result_q3 = generic.getMostCitedPublication()
result_q4 = generic.getMostCitedVenue()
result_q5 = generic.getVenuesByPublisherId('crossref:140')
result_q6 = generic.getPublicationInVenue('issn:1588-2861')
result_q7 = generic.getJournalArticlesInIssue(10, 17, 'doi:10.1371/journal.pbio.3000385')
result_q8 = generic.getJournalArticlesInVolume(2, 'doi:10.3233/ds-190016')
result_q9 = generic.getJournalArticlesInJournal('doi:10.3233/ds-190016')
result_q10 = generic.getProceedingsByEvent('oi')
result_q11 = generic.getPublicationAuthors('doi:10.1007/s11301-020-00196-4')
result_q12 = generic.getPublicationsByAuthorsName('maria')
#l = ['doi:10.1080/08989621.2020.1836620', 'doi:10.1007/s11301-020-00196-4']
#result_q13 = generic.getDistinctPublishersOfPublications(l) #I had problems in this last query and couldn't run it


print(result_q1)
print(result_q2)
print(result_q3)
print(result_q4)
print(result_q5)
print(result_q6)
print(result_q7)
print(result_q8)
print(result_q9)
print(result_q10)
print(result_q11)
print(result_q12)
#print(result_q13)
