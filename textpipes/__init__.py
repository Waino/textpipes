#!/usr/bin/env python
"""
textpipes - An experiment management system for NLP
"""
import logging


#__all__ = []

__version__ = '0.0.1'
__author__ = 'Stig-Arne Gronroos'
__author_email__ = "stig-arne.gronroos@aalto.fi"


def get_version():
    return __version__

# The public api imports need to be at the end of the file,
# so that the package global names are available to the modules
# when they are imported.

from .core import *
from . import components, check
from . import finnpos, anmt, wmt_sgm, morfessor, multiling, dummy, opennmt
from . import anmt_latent
from . import translation_analysis

# Most common rules for easy access
from .dedup import Deduplicate
from .counting import CountTokens
from .external import Concatenate
from .tabular import SplitColumns
from .truecaser import TrainTrueCaser, TrueCase

# Most common components for easy access
# more are available by importing from tp.components
from .components.preprocessing import *
from .components.filtering import MonoFilter, ParallelFilter, FilterByLength
from .components.tokenizer import Tokenize
from .components.core import *
