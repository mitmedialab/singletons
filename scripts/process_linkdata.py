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
import pymongo

# XML Config
pages = '../data/wikidatawiki-latest-pages-articles.xml'
prefix = '{http://www.mediawiki.org/xml/export-0.8/}'

# Threading config
# Threading with python threads degrades performance, probably because of GIL.
# Best to leave off for now and replace with multiprocessing and/or db sharding.
page_thread = False
link_thread = False

class Wikidata:
    """Loads wikidata from an xml dump and adds it to a database."""
    
    def __init__(self):
        """Initialize the parsing process."""
        # Connect to database
        self.mcon = pymongo.Connection('localhost')
        self.mdb = self.mcon.singletons
        # Create queues for raw page data and processed link objects
        self.page_q = Queue(500)
        self.link_q = Queue(50)
        # Get start time and init count
        self.start_time = time.time()
        self.page_count = 0;
        self.link_count = 0;
        # Buffers for batching insertions
        # 500k seems to give the best performance but causes the server to
        # disconnect
        self.link_batch = []
        self.link_batch_size = 250000
        
    def process_linkdata(self):
        """Parses the wikidata xml."""
        # Create task queue and worker threads
        if page_thread:
            t = Thread(target=self.page_worker)
            t.daemon = True
            t.start()
        if link_thread:
            t = Thread(target=self.link_worker)
            t.daemon = True
            t.start()
        # Create iterator and get root element
        context = iterparse(pages, events=('start', 'end'))
        event, root = context.next()
        # Iterate through pages
        for event, elem in context:
            if (event != 'end'):
                continue
            if elem.tag == "%stitle" % (prefix):
                entity = elem.text
            if elem.tag == "%sns" % (prefix):
                namespace = elem.text
            if elem.tag == "%stext" % (prefix):
                text = elem.text
            if elem.tag == "%spage" % (prefix):
                self.page_count += 1
                # We only want regular articles (namespace 0) not talk, categories, etc.
                if namespace == '0':
                    # Wikidata texts are sent to worker threads in batches so that
                    # json parsing etc. can be done while the xml parser is reading
                    # from disk.
                    if page_thread:
                        self.page_q.put((entity, text))
                    else:
                        self.process_page(entity, text)
                entity = text = ''
                ns = -3
                # Clear all parsed elements from RAM
                root.clear()
        # Process any links left in the buffer
        self.flush_links()
        # Wait for worker threads to complete
        if page_thread:
            self.page_q.join()
        if link_thread:
            self.link_q.join()
        print "Insertion complete"
        print "Creating index on: entity"
        self.mdb.langlinks.create_index('entity')
        print "Creating index on: (language, title)"
        self.mdb.langlinks.create_index([('language', pymongo.ASCENDING), ('title', pymongo.ASCENDING)])
    
    def page_worker(self):
        '''Processes a page from the queue.'''
        # Process pages until there are none left
        while True:
            # Get a page json string from the queue
            (entity, text) = self.page_q.get()
            self.process_page(entity, text)
            self.page_q.task_done()
    
    def process_page(self, entity, text):
        '''Creates language links for a page and adds them to a buffer.'''
        # Batch processed objects
        data = json.loads(text)
        try:
            # Skip disambiguation pages
            for claim in data['claims']:
                if claim['m'][1] == 107:
                    # Q11651459 is the disambiguation type
                    # Q4167410 is an article about disambiguation pages that
                    # is mistakenly used to flag disambiguation pages.
                    if claim['m'][3]['numeric-id'] in [4167410, 11651459]:
                        return
        except KeyError:
            pass
        try:
            titles = []
            for language, title in data['links'].items():
                # Create a json object to save to the db later
                #print("%s : %s : %s" % (title, key, value))
                # Change 'enwiki' to 'en'
                language_code = language[:-4]
                titles.append(u'%s:%s' % (language_code, title))
                self.link_batch.append({'entity':entity, 'language':language_code, 'title':title})
                if len(self.link_batch) >= self.link_batch_size:
                    self.flush_links()
        except KeyError:
            pass
        except AttributeError:
            pass
    
    def flush_links(self):
        '''Moves all buffered links into the insertion queue.'''
        if link_thread:
            self.link_q.put(self.link_batch)
        else:
            self.process_links_mongo(self.link_batch)
        self.link_batch = []
    
    def link_worker(self):
        '''Processes links from the insertion queue.'''
        # Process links until there are none left
        while True:
            self.process_links_mongo(self.link_q.get())
            self.link_q.task_done()
    
    def process_link_redis(self, link):
        '''Adds a link to a redis database.'''
        title = u'%s:%s' % (link['language'], link['title'])
        self.rdb.set(u'title:%s' % title, link['entity'])
        self.rdb.sadd(u'entity:%s' % (link['entity']), title)
    
    def process_links_mongo(self, link):
        '''Adds a link to a mongo database.'''
        while True:
            try:
                self.mdb.langlinks.insert(link)
                break
            except pymongo.errors.AutoReconnect:
                # Reconnect and retry.  This may add duplicates but we'll
                # create a unique index when we have finished.
                print "Caught AutoReconnect, reconnecting..."
                self.mcon = pymongo.Connection('localhost')
                self.mdb = self.mcon.singletons
                print "Reconnected"
        self.link_count += len(link)
        self.print_stats()
    
    def print_stats(self):
        '''Prints progress and performance info.'''
        elapsed = time.time() - self.start_time
        pps = self.page_count / elapsed
        lps = self.link_count / elapsed
        print "Pages: %dk(%d/s), Links: %dk(%d/s) PageQ:%d LinkQ:%d" % (self.page_count/1000, int(pps), self.link_count/1000, int(lps), self.page_q.qsize(), self.link_q.qsize())

# Run the script
wikidata = Wikidata()
wikidata.process_linkdata()
