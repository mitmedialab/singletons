# Cultural Singletons
# Create a database for storing language links and singleton articles
# Edward L. Platt <elplatt@mit.edu>
# MIT Center for Civic Media

# Third-party imports
from pymongo import Connection, ASCENDING, DESCENDING

# Drop the existing database and create new database and collecitons
con = Connection('localhost')
con.singletons.command('dropDatabase')
db = con.singletons
langlinks = db.langlinks
pages = db.pages
