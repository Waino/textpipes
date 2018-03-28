"""Pipes are composite Rules built out of Components.
Components are text processing operations expressed as Python generators.
"""

from .core import *
from . import demoses
from . import europarl
from . import estonian
from . import filtering
from . import newscorpus
#from . import nltk
from . import opensubtitles
from . import preprocessing
from . import segmentation
from . import subsampling
from . import tokenizer
