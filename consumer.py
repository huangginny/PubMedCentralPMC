''' Consumer for PubMed Central
    Takes in metadata in pmc formats '''

from lxml import etree
from datetime import date, timedelta
import requests
import time
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = 'pubmedcentralpmc'

NAMESPACES = {#'dc': 'http://purl.org/dc/elements/1.1/', 
            #'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/',
            'pmc': 'http://dtd.nlm.nih.gov/2.0/xsd/archivearticle'}

def consume(days_back=1):
    base_url = "http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords"
    start_date = TODAY - timedelta(days_back)
    url = base_url + "&metadataPrefix=pmc&from={}".format(str(start_date))
    print(url)

    records = get_records(url)
    results_list = []
    for record in records:
        doc_id = record.xpath("//ns0:identifier/node()", namespaces = NAMESPACES)[0]
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        results_list.append(RawDocument({
                        'doc': record,
                        'source': NAME,
                        'doc_id': doc_id,
                        'filetype': 'xml'
                    }))
    return results_list  # a list of raw documents

def get_records(url):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)
    return records


def normalize(raw_doc, timestamp):

    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)
    
    titles = doc.xpath("//pmc:title-group/pmc:article-title/node()", namespaces = NAMESPACES)
    title = ""
    for ti in titles:
        t = ti
        if isinstance(ti, etree._Element):
            for element in ti.getiterator():
                if element.tag.find('sup') == -1:
                    t = element.text
                else:
                    t = ""
        title += t
    
    descriptions = doc.xpath("//pmc:abstract/pmc:p/node()", namespaces = NAMESPACES)
    description = ""
    for dsc in descriptions:
        d = dsc
        if isinstance(dsc, etree._Element):
            for element in dsc.getiterator():
                d = element.text
        description += d
    #description.replace('\xe2','\n')
    
    
    url = ''
    service_id = doc.xpath("//ns0:header/ns0:identifier/node()", namespaces=NAMESPACES)[0]
    doi = doc.xpath("//pmc:article-id[@pub-id-type='doi']/node()", namespaces=NAMESPACES)
    pmid = doc.xpath("//pmc:article-id[@pub-id-type='pmid']/node()", namespaces=NAMESPACES)
    pmcid = doc.xpath("//pmc:article-id[@pub-id-type='pmcid']/node()", namespaces=NAMESPACES)
    if len(pmcid) == 1:
        pmcid = pmcid[0]
        url = 'http://www.ncbi.nlm.nih.gov/pmc/articles/' + pmcid
    if len(pmid) == 1:
        pmid = pmid[0]
        url = 'http://www.ncbi.nlm.nih.gov/pubmed/' + pmid
    if len(doi) == 0:
        doi = ''
    else:
        doi = doi[0]
        url = 'http://dx.doi.org/' + doi

    if url == '':
        raise Exception("No url provided!")
    ids = {'url':url, 'doi':doi, 'service_id':service_id}
    #print(ids)

    '''
    surname = doc.xpath('//pmc:contrib/pmc:name/pmc:surname/node()', namespaces=NAMESPACES)
    given_names = doc.xpath('//pmc:contrib/pmc:name/pmc:given-names/node()', namespaces=NAMESPACES)
    full_names = zip(surname, given_names)
    contributors = []
    contributors += [', '.join(names) for names in full_names]
    
    email_list = []
    email = doc.xpath('//pmc:contrib/pmc:email/node()', namespaces=NAMESPACES)

    if len(email) == len(contributors):
        email_list = email
    else:
        email_list.append('')
    
    contributors = zip(contributors, email_list)

    contributor_list = []
    for contributor in contributors:
        if type(contributor) == tuple:
            contributor_list.append({'full_name': contributor[0], 'email':contributor[1]})
        else:
            contributor_list.append({'full_name': contributor, 'email':''})

    contributor_list = contributor_list or [{'full_name': 'no contributors', 'email': ''}]
    '''
    contributor_list = []
    contributors = doc.xpath('//pmc:contrib-group/node()', namespaces=NAMESPACES)
    for contrib in contributors:
        if isinstance(contrib, etree._Element):
            surname = ""
            given_name = ""
            email = ""
            for element in contrib.getiterator():       
                if element.tag.find("surname") > -1:
                    surname = element.text
                elif element.tag.find("given-names") > -1:
                    given_name = element.text
                elif element.tag.find("email") > -1:
                    email = element.text
            if (surname != ""):
                fullname = surname + ", " + given_name
                contributor_list.append({'full_name': fullname, 'email': email})
    #print(contributor_list)

    
    tags_notitalic = doc.xpath("//pmc:kwd-group/pmc:kwd/node()", namespaces=NAMESPACES)
    tags = doc.xpath("//pmc:kwd-group/pmc:kwd/pmc:italic/node()", namespaces=NAMESPACES)
    for tag in tags_notitalic:
        if isinstance(tag, etree._Element) == False and tag.find('\n') == -1:
            tags.append(tag)
    year = doc.xpath("//pmc:pub-date[@pub-type='epub']/pmc:year/node()", namespaces=NAMESPACES) or \
            doc.xpath("//pmc:pub-date[@pub-type='ppub']/pmc:year/node()", namespaces=NAMESPACES)
    month = doc.xpath("//pmc:pub-date[@pub-type='epub']/pmc:month/node()", namespaces=NAMESPACES) or \
            doc.xpath("//pmc:pub-date[@pub-type='ppub']/pmc:month/node()", namespaces=NAMESPACES)
    day = doc.xpath("//pmc:pub-date[@pub-type='epub']/pmc:day/node()", namespaces=NAMESPACES) or \
            doc.xpath("//pmc:pub-date[@pub-type='ppub']/pmc:day/node()", namespaces=NAMESPACES)
    date_created = '{year}-{month}-{day}'.format(year=year[0], month=month[0], day=day[0])
    
    #print('title: ' + title[0])
    #print('author: ' + contributors_list)
    journal_ids = doc.xpath("//pmc:journal-id/node()", namespaces=NAMESPACES)
    journal_title = doc.xpath("//pmc:journal-title-group/pmc:journal-title/node()", namespaces=NAMESPACES)
    issn = doc.xpath("//pmc:issn/node()", namespaces=NAMESPACES)
    volume = (doc.xpath("//pmc:article-meta/pmc:volume/node()", namespaces=NAMESPACES) or [""])[0]
    issue = (doc.xpath("//pmc:article-meta/pmc:issue/node()", namespaces=NAMESPACES) or [""])[0]
    fpage = (doc.xpath("//pmc:article-meta/pmc:fpage/node()", namespaces=NAMESPACES) or [""])[0]
    lpage = (doc.xpath("//pmc:article-meta/pmc:lpage/node()", namespaces=NAMESPACES) or [""])[0]
    publisher = (doc.xpath("//pmc:publisher/pmc:publisher-name/node()", namespaces=NAMESPACES) or [""])[0]
    statement = (doc.xpath("//pmc:permissions/pmc:copyright-statement/node()", namespaces=NAMESPACES) or [""])[0]
    copyright_year = (doc.xpath("//pmc:permissions/pmc:copyright-year/node()", namespaces=NAMESPACES) or [""])[0]
    copyright_holder = (doc.xpath("//pmc:permissions/pmc:copyright-holder/node()", namespaces=NAMESPACES) or [""])[0]
    license_list = doc.xpath("//pmc:license/pmc:license-p/node()", namespaces=NAMESPACES)
    license = ""
    affliations = doc.xpath("//pmc:aff/pmc:institution/node()", namespaces=NAMESPACES)
    for lic in license_list:
        l = lic
        if isinstance(lic, etree._Element):
            for element in lic.getiterator():
                l = element.text
        license += l

    normalized_dict = {
            'title': title,
            'contributors': contributor_list,
            'properties': {
                'published-in': {
                    'journal-ids': journal_ids,
                    'journal-title': journal_title,
                    'issn': issn,
                    'volume': volume,
                    'issue': issue
                },
                'author-affiliations': affliations,
                'publisher': publisher,
                'permissions': {
                    'copyright-statement': statement,
                    'copyright-year': copyright_year,
                    'copyright-holder': copyright_holder,
                    'license': license
                }
            },
            'description': description,
            'meta': {},
            'id': ids,
            'tags': tags,
            'source': NAME,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }
    return NormalizedDocument(normalized_dict)
    

if __name__ == '__main__':
    lint(consume, normalize) 