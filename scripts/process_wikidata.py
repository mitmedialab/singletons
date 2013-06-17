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
disambig_map = {u'gu': [u'\u0ab8\u0a82\u0aa6\u0abf\u0a97\u0acd\u0aa7\u0ab6\u0ac0\u0ab0\u0acd\u0ab7\u0a95'], u'scn': [u'disambiguazzioni'], u'sco': [u'disambiguation'], u'zh-hk': [u'\u7dad\u57fa\u767e\u79d1\u6d88\u6b67\u7fa9\u9801'], u'zea': [u'deurverwiespagina'], u'pt-br': [u'desambigua\xe7\xe3o'], u'gl': [u'hom\xf3nimos'], u'lb': [u'homonymie'], u'la': [u'discretiva'], u'tr': [u'anlamayr\u0131m\u0131'], u'li': [u'verdudelikingspazjena'], u'lv': [u'vikip\u0113dijasnoz\u012bmjuatdal\u012b\u0161anaslapa'], u'tl': [u'paglilinaw'], u'th': [u'\u0e01\u0e32\u0e23\u0e41\u0e01\u0e49\u0e04\u0e27\u0e32\u0e21\u0e01\u0e33\u0e01\u0e27\u0e21'], u'te': [u'\u0c05\u0c2f\u0c4b\u0c2e\u0c2f\u0c28\u0c3f\u0c35\u0c43\u0c24\u0c4d\u0c24\u0c3f'], u'mwl': [u'zambigua\xe7on'], u'yi': [u'\u05d1\u05d0\u05d3\u05d9\u05d9\u05d8\u05df'], u'ceb': [u'mgapulongngamaylabawpasausakakahulogan'], u'de': [u'wikipedia-begriffskl\xe4rungsseite'], u'da': [u'flertydigetitler'], u'bar': [u'begriffsklearung'], u'zh-hans': [u'\u7ef4\u57fa\u767e\u79d1\u6d88\u6b67\u4e49\u9875'], u'de-ch': [u'begriffskl\xe4rung'], u'zh-hant': [u'\u7dad\u57fa\u767e\u79d1\u6d88\u6b67\u7fa9\u9801'], u'map-bms': [u'disambiguasi'], u'el': [u'\u03b1\u03c0\u03bf\u03c3\u03b1\u03c6\u03ae\u03bd\u03b9\u03c3\u03b7'], u'eo': [u'apartigiloj'], u'en': [u'disambiguation', u'disambiguation', u'dab', u'disambig'], u'zh': [u'\u6d88\u6b67\u4e49'], u'rmy': [u'dudalipen'], u'mdf': [u'\u043b\u0430\u043c\u0430\u0441\u043c\u0443\u0441\u0442\u044c'], u'eu': [u'argipenorri'], u'et': [u't\xe4psustuslehek\xfclg'], u'es': [u'desambiguaci\xf3n', u'ambig\xfcedadent\xedtulos', u'p\xe1ginadedesambiguaci\xf3ndewikipedia', u'p\xe1ginadedesambiguaci\xf3n'], u'en-gb': [u'disambiguation'], u'ru': [u'\u0441\u043f\u0438\u0441\u043e\u043a\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0439\u0432\u0432\u0438\u043a\u0438\u043f\u0435\u0434\u0438\u0438', u'\u043d\u0435\u043e\u0434\u043d\u043e\u0437\u043d\u0430\u0447\u043d\u043e\u0441\u0442\u044c', u'disambiguation', u'\u043d\u0435\u043e\u0434\u043d\u043e\u0437\u043d\u0430\u0447\u043d\u043e\u0441\u0442\u044c', u'\u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0439', u'\u043e\u043c\u043e\u043d\u0438\u043c\u0438\u044f'], u'zh-cn': [u'\u7ef4\u57fa\u767e\u79d1\u6d88\u6b67\u4e49\u9875'], u'ro': [u'dezambiguizare'], u'bn': [u'\u09a6\u09cd\u09ac\u09cd\u09af\u09b0\u09cd\u09a5\u09a4\u09be\u09a8\u09bf\u09b0\u09b8\u09a8'], u'be': [u'\u0441\u043f\u0456\u0441\u0437\u043d\u0430\u0447\u044d\u043d\u043d\u044f\u045e\u0443\u0432\u0456\u043a\u0456\u043f\u0435\u0434\u044b\u0456', u'\u043d\u0435\u0430\u0434\u043d\u0430\u0437\u043d\u0430\u0447\u043d\u0430\u0441\u0446\u044c', u'\u0430\u043c\u0430\u043d\u0456\u043c\u0456\u044f', u'\u0441\u0442\u0430\u0440\u043e\u043d\u043a\u0430\u0437\u043d\u0430\u0447\u044d\u043d\u043d\u044f\u045e', u'disambiguation', u'\u043d\u0435\u0430\u0434\u043d\u0430\u0437\u043d\u0430\u0447\u043d\u0430\u0441\u0446\u044c'], u'bg': [u'\u043f\u043e\u044f\u0441\u043d\u0438\u0442\u0435\u043b\u043d\u0430\u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430'], u'ms': [u'nyahkekaburan'], u'wa': [u'omonimeye'], u'ast': [u'p\xe1xinadedixebra'], u'zh-sg': [u'\u7ef4\u57fa\u767e\u79d1\u6d88\u6b67\u4e49\u9875'], u'jv': [u'disambiguasi'], u'br': [u'dishe\xf1velout'], u'ja': [u'\u30a6\u30a3\u30ad\u30da\u30c7\u30a3\u30a2\u306e\u66d6\u6627\u3055\u56de\u907f\u30da\u30fc\u30b8', u'\u66d6\u6627\u3055\u56de\u907f'], u'ilo': [u'panangilawlawag'], u'oc': [u'omonimia'], u'de-at': [u'begriffskl\xe4rung'], u'nds': [u'mehrd\xfcdigbegreep'], u'yue': [u'\u641e\u6e05\u695a'], u'simple': [u'disambiguation'], u'ca': [u'p\xe0ginadedesambiguaci\xf3'], u'cy': [u'gwahaniaethu'], u'cs': [u'rozcestn\xedk', u'rozcestn\xedk'], u'mzn': [u'\u06af\u062c\u06af\u062c\u06cc\u0628\u06cc\u062a\u0646'], u'pt': [u'desambigua\xe7\xe3o'], u'zh-tw': [u'\u7dad\u57fa\u767e\u79d1\u6d88\u6b67\u7fa9\u9801'], u'lt': [u'nuorodiniaistraipsniai'], u'pl': [u'stronaujednoznaczniaj\u0105ca'], u'nrm': [u'frouque'], u'hr': [u'razdvojba'], u'zh-my': [u'\u7ef4\u57fa\u767e\u79d1\u6d88\u6b67\u4e49\u9875'], u'hu': [u'wikip\xe9dia-egy\xe9rtelm\u0171s\xedt\u0151lap'], u'hi': [u'\u092c\u0939\u0941\u0935\u093f\u0915\u0932\u094d\u092a\u0940\u0936\u092c\u094d\u0926'], u'zh-mo': [u'\u7dad\u57fa\u767e\u79d1\u6d88\u6b67\u7fa9\u9801'], u'an': [u'pachinadedesambigaci\xf3n'], u'he': [u'\u05e4\u05d9\u05e8\u05d5\u05e9\u05d5\u05e0\u05d9\u05dd', u'\u05d5\u05d9\u05e7\u05d9\u05e4\u05d3\u05d9\u05d4\u05e4\u05d9\u05e8\u05d5\u05e9\u05d5\u05e0\u05d9\u05dd', u'\u05e4\u05d9\u05e8\u05d5\u05e9\u05d5\u05e0\u05d9\u05dd\u05d5\u05d9\u05e7\u05d9\u05e4\u05d3\u05d9\u05d4'], u'ml': [u'\u0d35\u0d3f\u0d35\u0d15\u0d4d\u0d37\u0d15\u0d7e'], u'stq': [u'bigriepskloorenge'], u'uk': [u'\u043d\u0435\u043e\u0434\u043d\u043e\u0437\u043d\u0430\u0447\u043d\u0456\u0441\u0442\u044c'], u'sr': [u'\u0432\u0438\u0448\u0435\u0437\u043d\u0430\u0447\u043d\u0430\u043e\u0434\u0440\u0435\u0434\u043d\u0438\u0446\u0430\u043d\u0430\u0432\u0438\u043a\u0438\u043f\u0435\u0434\u0438\u0458\u0438'], u'af': [u'wikipediadubbelsinnigheidsblad', u'dubbelsinnigheid'], u'vi': [u'\u0111\u1ecbnhh\u01b0\u1edbng'], u'is': [u'a\xf0greiningars\xed\xf0ur'], u'it': [u'disambigua', u'disambiguazione', u'omonimia'], u'vo': [u'telpl\xe4nov'], u'as': [u'\u09a6\u09cd\u09ac\u09cd\u09af\u09f0\u09cd\u09a5\u09a4\u09be\u09a6\u09c2\u09f0\u09c0\u0995\u09f0\u09a3'], u'ar': [u'\u062a\u0648\u0636\u064a\u062d'], u'io': [u'homonimo'], u'ia': [u'disambiguation'], u'az': [u'd\u0259qiql\u0259\u015fdirm\u0259'], u'id': [u'disambiguasi'], u'nl': [u'doorverwijspagina'], u'nn': [u'wikipedia-fleirtydingsside'], u'no': [u'flertydigetitler'], u'nb': [u'wikipedia-pekerside', u'hh-peker'], u'ne': [u'\u092c\u0939\u0941\u0935\u093f\u0915\u0932\u094d\u092a\u0940\u0936\u092c\u094d\u0926'], u'fr': [u"page d'homonymie de wikip\xe9dia", u'homonymie'], u'zh-yue': [u'\u641e\u6e05\u695a'], u'sv': [u's\xe4rskiljning'], u'fa': [u'\u0635\u0641\u062d\u0647\u0654\u0627\u0628\u0647\u0627\u0645\u200c\u0632\u062f\u0627\u06cc\u06cc'], u'fi': [u't\xe4smennyssivu'], u'de-formal': [u'wikipedia-begriffskl\xe4rungsseite'], u'en-ca': [u'disambiguation'], u'ka': [u'\u10d5\u10d8\u10d9\u10d8\u10de\u10d4\u10d3\u10d8\u10d8\u10e1\u10db\u10e0\u10d0\u10d5\u10d0\u10da\u10db\u10dc\u10d8\u10e8\u10d5\u10dc\u10d4\u10da\u10dd\u10d1\u10d8\u10e1\u10d2\u10d5\u10d4\u10e0\u10d3\u10d8'], u'ckb': [u'\u0695\u0648\u0648\u0646\u06a9\u0631\u062f\u0646\u06d5\u0648\u06d5'], u'roa-tara': [u'disambigua'], u'sq': [u'kthjellime'], u'ko': [u'\ub3d9\uc74c\uc774\uc758\uc5b4\ubb38\uc11c'], u'kn': [u'\u0ca6\u0ccd\u0cb5\u0c82\u0ca6\u0ccd\u0cb5\u0ca8\u0cbf\u0cb5\u0cbe\u0cb0\u0ca3\u0cc6'], u'su': [u'disambiguasi'], u'sk': [u'rozli\u0161ovaciastr\xe1nka'], u'sh': [u'vi\u0161ezna\u010dnaodrednica'], u'sl': [u'razlo\u010ditev']}
disambig_tags = disambig_map['en'] + disambig_map['es']

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
        if self.is_disambiguation(text):
            return False
        return True
    
    def is_disambiguation(self, text):
        tags = self.find_tags(text)
        for tag in tags:
            for disambig in disambig_tags:
                tag = tag.lower().strip()
                if disambig in tag or tag in disambig:
                    print 'skipping tag %s' % tag
                    return True
        return False
    
    def find_tags(self, text):
        return re.findall('{{(.+?)}}', text)
    
    def text_link_count(self, text):
        """Returns the number of old-style language links in the text."""
        links = re.findall(r'\[\[(.+?):.+?\]\]', text)
        links = [x for x in links if x.lower() in codes]
        return len(links)
    
# Run the script
wikipedia = Wikipedia()
wikipedia.process_wikidata()
