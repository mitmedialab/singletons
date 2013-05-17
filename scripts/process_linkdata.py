# Cultural Singletons
# Create database of language links from 'wikidata-latest-pages-articles' XML
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Standard imports
import sys
import json
import re
import time
from Queue import Queue
from threading import Thread
from xml.etree.cElementTree import ElementTree, iterparse

# Third party imports
from pymongo import Connection

# XML Config
pages = '../data/wikidatawiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

# Create queues for raw page data and processed link objects
page_q = Queue(500)
link_q = Queue(50)

# Get start time and init count
start_time = time.time()
page_count = 0;
link_count = 0;

# Page processor thread
def page_worker():
    global start_time
    global page_count
    global link_count
    # Open database connection
    con = Connection('localhost')
    db = con.singletons
    langlinks = db.langlinks
    # Batch processed objects
    batch = []
    batch_size = 10000;
    start = 0;
    # Process pages until there are none left
    while True:
        # Get a page json string from the queue
        (index, title, text) = page_q.get()
        data = json.loads(text)
        try:
            # Skip disambiguation pages
            if u'107' in data['claims']['m']:
                continue;
        except KeyError:
            pass
        try:
            for key, value in data['links'].items():
                # Create a json object to save to the db later
                #print("%s : %s : %s" % (title, key, value))
                batch.append({'canonical':title, 'language':key, 'title':value})
                if len(batch) >= batch_size:
                    link_q.put(batch)
                    link_count += len(batch)
                    batch = []
                    start = index + 1
        except KeyError:
            pass
        except AttributeError:
            pass
        # Notify the queue that the batch is complete
        page_q.task_done()

# Worker thread to insert created objects into the database
def link_worker():
    # Connect to database
    con = Connection('localhost')
    db = con.singletons
    langlinks = db.langlinks
    # Process links until there are none left
    while True:
        langlinks.insert(link_q.get())
        link_q.task_done()

def process_linkdata():
    global start_time
    global page_count
    global link_count
    # Create task queue and worker threads
    for i in range(1):
        t = Thread(target=page_worker)
        t.daemon = True
        t.start()
    for i in range(1):
        t = Thread(target=link_worker)
        t.daemon = True
        t.start()
        
    # Open database connection
    con = Connection('localhost')
    db = con.singletons
    langlinks = db.langlinks
    
    # Create iterator and get root element
    context = iterparse(pages, events=('start', 'end'))
    event, root = context.next()
    
    # Iterate through pages
    for event, elem in context:
        if (event != 'end'):
            continue
        if elem.tag == "%stitle" % (prefix):
            title = elem.text
        if elem.tag == "%sns" % (prefix):
            namespace = elem.text
        if elem.tag == "%stext" % (prefix):
            text = elem.text
        if elem.tag == "%spage" % (prefix):
            page_count += 1
            # We only want regular articles (namespace 0) not talk, categories, etc.
            if namespace == '0':
                # Wikidata texts are sent to worker threads in batches so that
                # json parsing etc. can be done while the xml parser is reading
                # from disk.
                page_q.put((page_count, title, text))
            title = text = ''
            ns = -3
            # Clear all parsed elements from RAM
            root.clear()
            elapsed = time.time() - start_time
            pps = page_count / elapsed
            lps = link_count / elapsed
            print "P: %dk(%d/s), L: %dk(%d/s) PQ:%d LQ:%d" % (page_count/1000, int(pps), link_count/1000, int(lps), page_q.qsize(), link_q.qsize())
    page_q.join()
    link_q.join()
    print "Insertion complete"
    print "Creating index on: canonical"
    langlinks.create_index('canonical')
    print "Creating index on: (language, title)"
    langlinks.create_index([('language', ASCENDING), ('title', ASCENDING)])

process_linkdata()
