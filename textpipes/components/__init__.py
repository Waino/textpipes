"""Pipes are composite Rules built out of Components.
Components are text processing operations expressed as Python generators.
"""

from .core import *
from . import europarl
from . import filtering
from . import newscorpus
#from . import nltk
from . import preprocessing
from . import subsampling
from . import tokenizer
from . import truecaser
