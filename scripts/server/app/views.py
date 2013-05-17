from flask import render_template, request
from app import app, db
from forms import PagerForm
import re

@app.route('/', methods=['GET'])
@app.route('/index', methods=['GET'])
def index():
    per_page = int(request.args.get('per_page', 25))
    start = int(request.args.get('start', '0'))
    if request.args.get('submit', '') == 'First':
        start = 0
    if request.args.get('submit', '') == 'Prev':
        start = max(0, start - per_page)
    elif request.args.get('submit', '') == 'Next':
        start = start + per_page
    end = start + per_page - 1
    pages = [(x['title'], re.sub(r' ', '_', x['title'])) for x in db.pages.find(skip=start, limit=per_page)]
    return render_template("index.html", start=start, end=end, per_page=per_page, total=db.pages.count(), pages=pages)
