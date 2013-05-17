from flask import Flask
from pymongo import Connection

app = Flask(__name__)
con = Connection('localhost')
db = con.singletons
from app import views
