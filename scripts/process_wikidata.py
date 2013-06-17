#!/usr/bin/python
# coding=utf8

# Cultural Singletons
# Look for singletons in 'latest-pages-articles' XML dump
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Standard imports
import sys
import re
from xml.etree.cElementTree import ElementTree, iterparse

# Third-party imports
import pymongo

# XML Config
pages_path = '../data/eswiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

# Language codes
codes = ['aa', 'ab', 'ace', 'af', 'ak', 'als', 'am', 'ang', 'an', 'arc', 'ar', 'arz', 'ast', 'as', 'av', 'ay', 'az', 'bar', 'bat_smg', 'ba', 'bcl', 'be_x_old', 'be', 'bg', 'bh', 'bi', 'bjn', 'bm', 'bn', 'bo', 'bpy', 'br', 'bs', 'bug', 'bxr', 'ca', 'cbk_zam', 'cdo', 'ceb', 'ce', 'cho', 'chr', 'ch', 'chy', 'ckb', 'co', 'crh', 'cr', 'csb', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'diq', 'dsb', 'dv', 'dz', 'ee', 'el', 'eml', 'en', 'eo', 'es', 'et', 'eu', 'ext', 'fa', 'ff', 'fiu_vro', 'fi', 'fj', 'fo', 'frp', 'frr', 'fr', 'fur', 'fy', 'gag', 'gan', 'ga', 'gd', 'glk', 'gl', 'gn', 'got', 'gu', 'gv', 'hak', 'ha', 'haw', 'he', 'hif', 'hi', 'ho', 'hr', 'hsb', 'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'ilo', 'io', 'is', 'it', 'iu', 'ja', 'jbo', 'jv', 'kaa', 'kab', 'ka', 'kbd', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn', 'koi', 'ko', 'krc', 'kr', 'ksh', 'ks', 'ku', 'kv', 'kw', 'ky', 'lad', 'la', 'lbe', 'lb', 'lez', 'lg', 'lij', 'li', 'lmo', 'ln', 'lo', 'ltg', 'lt', 'lv', 'map_bms', 'mdf', 'mg', 'mhr', 'mh', 'min', 'mi', 'mk', 'ml', 'mn', 'mo', 'mrj', 'mr', 'ms', 'mt', 'mus', 'mwl', 'myv', 'my', 'mzn', 'nah', 'nap', 'na', 'nds_nl', 'nds', 'ne', 'new', 'ng', 'nl', 'nn', 'nov', 'no', 'nrm', 'nso', 'nv', 'ny', 'oc', 'om', 'or', 'os', 'pag', 'pam', 'pap', 'pa', 'pcd', 'pdc', 'pfl', 'pih', 'pi', 'pl', 'pms', 'pnb', 'pnt', 'ps', 'pt', 'qu', 'rm', 'rmy', 'rn', 'roa_rup', 'roa_tara', 'ro', 'rue', 'ru', 'rw', 'sah', 'sa', 'scn', 'sco', 'sc', 'sd', 'se', 'sg', 'sh', 'simple', 'si', 'sk', 'sl', 'sm', 'sn', 'so', 'sq', 'srn', 'sr', 'ss', 'stq', 'st', 'su', 'sv', 'sw', 'szl', 'ta', 'tet', 'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tpi', 'tr', 'ts', 'tt', 'tum', 'tw', 'ty', 'udm', 'ug', 'uk', 'ur', 'uz', 'vec', 'vep', 've', 'vi', 'vls', 'vo', 'war', 'wa', 'wo', 'wuu', 'xal', 'xh', 'xmf', 'yi', 'yo', 'za', 'zea', 'zh_classical', 'zh_min_nan', 'zh_yue', 'zh', 'zu']

# Disambiguation tags
disambiguation = [u'{{täsmennyssivu}}', u'{{mga pulong nga may labaw pa sa usa ka kahulogan}}', u'{{ابهام‌زدایی}}', u'{{disambiguasi}}', u'{{Định hướng}}', u'{{deurverwiespagina}}', u'{{frouque}}', u'{{panangilawlawag}}', u'{{homónimos}}', u'{{verdudelikingspazjena}}', u'{{అయోమయ నివృత్తి}}', u'{{višeznačna odrednica}}', u'{{egyértelműsítő lapok}}', u'{{disambiguasi}}', u'{{disambiguasi}}', u'{{doorverwijspagina}}', u'{{ڕوونکردنەوە}}', u'{{razločitev}}', u'{{paglilinaw}}', u'{{nozīmju atdalīšana}}', u'{{Вишезначна одредница}}', u'{{দ্ব্যৰ্থতা দূৰীকৰণ}}', u'{{Лама смусть}}', u'{{flertydige titler}}', u'{{dudalipen}}', u'{{बहुविकल्पी शब्द}}', u'{{nyahkekaburan}}', u'{{desambiguação}}', u'{{homonimo}}', u'{{দ্ব্যর্থতা নিরসন}}', u'{{dezambiguizare}}', u'{{omonimia}}', u'{{homonymie}}', u'{{zambiguaçon}}', u'{{disambiguation}}', u'{{särskiljning}}', u'{{באדייטן}}', u'{{homonymie}}', u'{{disambiguasi}}', u'{{argipen orri}}', u'{{discretiva}}', u'{{mehrdüdig begreep}}', u'{{bigriepskloorenge}}', u'{{સંદિગ્ધ શીર્ષક}}', u'{{disambiguazzioni}}', u'{{ದ್ವಂದ್ವ ನಿವಾರಣೆ}}', u'{{nuorodiniai straipsniai}}', u'{{گجگجی بیتن}}', u'{{曖昧さ回避}}', u'{{fleirtyding}}', u'{{ambigüedad en títulos}}', u'{{rozlišovacia stránka}}', u'{{消歧义}}', u'{{omonimeye}}', u'{{páxina de dixebra}}', u'{{การแก้ความกำกวม}}', u'{{dubbelsinnigheid}}', u'{{telplänov}}', u'{{rozcestníky}}', u'{{begriffsklärung}}', u'{{disambigua}}', u'{{razdvojba}}', u'{{aðgreiningarsíður}}', u'{{apartigiloj}}', u'{{Неоднозначність}}', u'{{disambigua}}', u'{{pachina de desambigación}}', u'{{Αποσαφήνιση}}', u'{{dəqiqləşdirmə}}', u'{{बहुविकल्पी शब्द}}', u'{{disambiguation}}', u'{{Неоднозначность}}', u'{{동음이의어 문서}}', u'{{täpsustuslehekülg}}', u'{{strona ujednoznaczniająca}}', u'{{搞清楚}}', u'{{kthjellime}}', u'{{begriffsklearung}}', u'{{pàgina de desambiguació}}', u'{{توضيح}}', u'{{disambiguation}}', u'{{വിവക്ഷകൾ}}', u'{{gwahaniaethu}}', u'{{Пояснителна страница}}', u'{{disheñvelout}}', u'{{anlam ayrımı}}', u'{{פירושונים}}']

class Wikipedia:
    
    def __init__(self):
        """Initialize the parsing process."""
        # Connect to db
        self.mcon = pymongo.Connection('localhost')
        self.mdb = self.mcon.singletons
    
    def ensure_unicode(self, s):
        """Returns s as a unicode string."""
        if isinstance(s, unicode):
            return s
        return s.decode('ascii')
    
    def process_wikidata(self):
        """Process the wikipedia xml."""
        # Create iterator
        context = iterparse(pages_path, events=('start', 'end'))
        event, root = context.next()
        # Iterate through pages
        namespace = -3
        title = ''
        text = ''
        entity = ''
        redirect = ''
        count = 0
        for event, elem in context:
            if event != 'end':
                continue
            if elem.tag == "%sns" % (prefix):
                namespace = self.ensure_unicode(elem.text)
            elif elem.tag == "%stitle" % (prefix):
                title = self.ensure_unicode(elem.text)
            elif elem.tag == "%sredirect" % (prefix):
                redirect = self.ensure_unicode(elem.get('title'))
            elif elem.tag == "%stext" % (prefix):
                if (elem.text != None):
                    text = self.ensure_unicode(elem.text)
            elif elem.tag == "%spage" % (prefix):
                sys.stdout.write('.')
                if len(redirect) < 1 and namespace == u'0':
                    link = self.mdb.langlinks.find_one({'language':'es', 'title':title})
                    if self.is_singleton(title, link, text):
                        sys.stdout.write("\n")
                        count += 1
                        print u'%s %s SINGLETON (Found %d)' % (entity, title, count)
                        self.mdb.pages.insert({'language':'es', 'title':title})
                namespace = -3
                title = ''
                text = ''
                entity = ''
                redirect = ''
                # Clear parsed xml elements from RAM
                root.clear()
        print "Parsed all pages"
        print "Creating index on: language"
        self.mdb.pages.create_index('language')
        print "Creating index on: canonical"
        self.mdb.pages.create_index('canonical')
        
    def is_singleton(self, title, link, text):
        article_count = 0
        # Get language links
        if link:
            entity = self.ensure_unicode(link['entity'])
            if (link['disambiguation'] == 1):
                return False
            article_count = self.mdb.langlinks.find({'entity':entity}).count()
        if article_count > 1:
            return False
        if self.text_link_count(text) > 0:
            print "Skipping %s with old style links" % (title)
            return False
        if any(tag in text.lower() for tag in disambiguation):
            print "Skipping %s with disambiguation tag" % (title)
            return False
        return True
        
    def text_link_count(self, text):
        """Returns the number of old-style language links in the text."""
        links = re.findall(r'\[\[(.+?):.+?\]\]', text)
        links = [x for x in links if x.lower() in codes]
        if len(links) > 0:
            print links
        return len(links)
    
# Run the script
wikipedia = Wikipedia()
wikipedia.process_wikidata()
