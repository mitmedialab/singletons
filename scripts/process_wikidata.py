# Cultural Singletons
# Look for singletons in 'latest-pages-articles' XML dump
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Standard imports
import re
from xml.etree.cElementTree import ElementTree, iterparse

# Third-party imports
from pymongo import Connection

# XML Config
pages = '../data/eswiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

# Connect to db
con = Connection('localhost')
db = con.singletons
langlinks = db.langlinks
pages = db.pages

# Create iterator
context = iterparse(pages, events=('start', 'end'))
event, root = context.next()

# Iterate through pages
namespace = -3
for event, elem in context:
    if event != 'end':
        continue
    if elem.tag == "%sns" % (prefix):
        namespace = elem.text
    if elem.tag == "%stitle" % (prefix):
        title = elem.text
    if elem.tag == "%spage" % (prefix):
        print "page: %s" % title
        if namespace != '0':
            continue
        articles = 0
        # Get language links
        link = langlinks.find_one({'language':'eswiki', 'title':title})
        if link:
            print 'Found link with canonical %s' % link['canonical']
            canonical = link['canonical']
            articles = langlinks.find({'canonical':canonical}).count()
        if articles > 1:
            print title
        else:
            print title
            print '    singleton'
            #db.singletons.insert({'language':'es', 'title':title})
        namespace = -3
        title = ''
        # Clear parsed xml elements from RAM
        root.clear()
pages.create_index('language')
pages.create_index('canonical')

