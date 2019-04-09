import json
from urllib import request
from lib.config import get_config


class Solr:
    limit = 100
    host = get_config('solr', 'host')
    port = get_config('solr', 'port')
    core = get_config('solr', 'core')
    solr_url = 'http://' + host + ':' + port + '/solr/' + core + '/'

    @classmethod
    def facet_duplicate_doc(cls):
        """
        This function returns duplicate documents by Solr facet.
        :return: Document list
        """
        url = cls.solr_url + 'select?facet.field=TC_ID&facet=on&q=*:*&rows=0&alt=json'
        print(url)
        req = request.Request(url)
        res = request.urlopen(req)
        res = res.read()
        res = json.loads(res)
        return res['facet_counts']['facet_fields']['TC_ID']

    @classmethod
    def query_doc_list(cls, tc_id):
        """
        This function returns documents by given TC_ID
        :return:
        """
        url = cls.solr_url + 'select?q=TC_ID:' + tc_id + '&rows=100&alt=json'
        print(url)
        req = request.Request(url)
        res = request.urlopen(req)
        res = res.read()
        res = json.loads(res)
        return res

    @classmethod
    def delete_doc(cls, solr_id):
        """
        This function delete document by given solr id
        :param solr_id:
        :return:
        """
        url = cls.solr_url + 'update?commitWithin=1000&overwrite=true&wt=json'
        data = """<add><delete><query>id:"%s"</query></delete><commit/></add>""" % solr_id
        header = {'Content-type': 'text/xml; charset=utf-8'}
        req = request.Request(url=url, data=data.encode(encoding='utf-8'), headers=header)
        res = request.urlopen(req)
        res = res.read()
        return res
