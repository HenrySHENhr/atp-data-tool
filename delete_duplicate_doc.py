import time
from lib.solr import Solr


def delete_doc_by_facet():
    docs = Solr.facet_duplicate_doc()
    if len(docs) > 1 and int(docs[1]) > 1:
        for i in range(int(len(docs) / 2)):
            index = i * 2
            tc_id = docs[index]
            facet_count = docs[index + 1]
            print('tc_id: ' + tc_id + ' count: ' + str(facet_count))
            if int(facet_count) < 2:
                return True
            else:
                duplicate_doc = Solr.query_doc_list(tc_id)
                if int(duplicate_doc['response']['numFound']) > 1:
                    duplicate_docs = duplicate_doc['response']['docs']
                    remove_docs_id = []
                    latest_time = -1
                    latest_doc_id = ''
                    for remove_doc in duplicate_docs:
                        print('date: ' + remove_doc['date'] + ' id: ' + remove_doc['id'])
                        sec = time.mktime(time.strptime(remove_doc['date'], "%Y-%m-%dT%H:%M:%SZ"))
                        if latest_time == -1:
                            latest_time = sec
                            latest_doc_id = remove_doc['id']
                        else:
                            if latest_time >= sec:
                                remove_docs_id.append(remove_doc['id'])
                            else:
                                remove_docs_id.append(latest_doc_id)
                                latest_time = sec
                                latest_doc_id = remove_doc['id']
                    for remove_id in remove_docs_id:
                        print('remove: ' + remove_id)
                        Solr.delete_doc(remove_id)
        delete_doc_by_facet()


if __name__ == '__main__':
    delete_doc_by_facet()
