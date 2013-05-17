# Cultural Singletons
# Look for singletons in 'latest-pages-articles' XML dump
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Standard imports
import sys
import re
from xml.etree.cElementTree import ElementTree, iterparse

# Third-party imports
from pymongo import Connection

# XML Config
pages_path = '../data/eswiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

def ensure_unicode(s):
    if isinstance(s, unicode):
        return s
    return s.decode('ascii')

# Connect to db
con = Connection('localhost')
db = con.singletons
langlinks = db.langlinks
pages = db.pages

# Create iterator
context = iterparse(pages_path, events=('start', 'end'))
event, root = context.next()

# Iterate through pages
namespace = -3
title = ''
canonical = ''
redirect = ''
count = 0
for event, elem in context:
    if event != 'end':
        continue
    if elem.tag == "%sns" % (prefix):
        namespace = ensure_unicode(elem.text)
    elif elem.tag == "%stitle" % (prefix):
        title = ensure_unicode(elem.text)
    elif elem.tag == "%sredirect" % (prefix):
        redirect = ensure_unicode(elem.get('title'))
    elif elem.tag == "%spage" % (prefix):
        sys.stdout.write('.')
        if len(redirect) < 1 and namespace == u'0':
            article_count = 0
            # Get language links
            link = langlinks.find_one({'language':'eswiki', 'title':title})
            if link:
                canonical = ensure_unicode(link['canonical'])
                article_count = langlinks.find({'canonical':canonical}).count()
            if article_count > 1:
                pass
            else:
                sys.stdout.write("\n")
                count += 1
                print u'%s %s SINGLETON (%d) (Found %d)' % (canonical, title, article_count, count)
                db.singletons.insert({'language':'es', 'title':title, 'link_count':article_count})
        namespace = -3
        title = ''
        canonical = ''
        redirect = ''
        # Clear parsed xml elements from RAM
        root.clear()
print "Parsed all pages"
print "Creating index on: language"
pages.create_index('language')
print "Creating index on: canonical"
pages.create_index('canonical')
