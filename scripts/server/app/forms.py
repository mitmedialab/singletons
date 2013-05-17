from flask.ext.wtf import Form, TextField
from flask.ext.wtf import Required

class PagerForm(Form):
    start = TextField('start', validators=[Required()], default="0")
    per_page = TextField('per_page', validators=[Required()], default="25")
