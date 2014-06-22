from flask.ext.wtf import Form
from wtforms import TextField
from wtforms.validators import Required
    
class PosForm(Form):
    add_pos_domain = TextField('add_pos_domain', validators = [Required()])
    del_pos_domain = TextField('del_pos_domain')

class NegForm(Form):
    add_neg_domain = TextField('add_neg_domain', validators = [Required()])
    del_neg_domain = TextField('del_neg_domain')

class SlidersForm(Form):
    links_slider = TextField('links_slider')
    alexa_slider = TextField('alexa_slider')
    p_rank_slider = TextField('p_rank_slider')
    in_slider = TextField('in_slider')
    out_slider = TextField('out_slider')
    self_slider = TextField('self_slider')

