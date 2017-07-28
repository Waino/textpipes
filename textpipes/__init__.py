#!/usr/bin/env python
"""
textpipes - An experiment management system for NLP
"""
import logging


#__all__ = []

__version__ = '0.0.1'
__author__ = 'Stig-Arne Gronroos'
__author_email__ = "stig-arne.gronroos@aalto.fi"

_logger = logging.getLogger(__name__)


def get_version():
    return __version__

# The public api imports need to be at the end of the file,
# so that the package global names are available to the modules
# when they are imported.

from .recipe import Recipe
from .configuration import Config
from . import rules, components, check

# Most common rules for easy access
from .rules.external import Concatenate
from .rules.dedup import Deduplicate
from .rules.core import *

# Most common components for easy access
# more are available by importing from tp.components
from .components.preprocessing import *
from .components.filtering import MonoFilter, ParallelFilter, FilterByLength
from .components.core import *
