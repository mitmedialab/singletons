# Cultural Singletons
# Find JSON data for a single link
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Standard imports
import json
import re
from xml.etree.cElementTree import ElementTree, iterparse

# XML Import
pages = '../data/wikidatawiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

# Create iterator and get root element
context = iterparse(pages, events=('start', 'end'))
event, root = context.next()

# Iterate through pages
i = 0
for event, elem in context:
    if (event != 'end'):
        continue
    if elem.tag == "%stitle" % (prefix):
        title = elem.text
    if elem.tag == "%stext" % (prefix):
        text = elem.text
    if elem.tag == "%spage" % (prefix):
        if i % 100000 == 0:
            print i
        i += 1;
        if title == 'Q15':
            print json.loads(text)
            break
        title = text = ''
        # Clear root element from RAM
        root.clear()
