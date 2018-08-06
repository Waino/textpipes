from ..core.utils import read_lang_file
from .core import RegexSubstitution
from .filtering import Filter, FilterRegex, ParallelFilter, NoFilter


class RemoveLanguageTags(RegexSubstitution):
    def __init__(self, **kwargs):
        super().__init__([(r'^([\.-] )*\([A-Za-z][A-Za-z]\) ', '')], **kwargs)


class FilterContractions(FilterRegex):
    """
    Europarl contains parts that have been left untranslated,
    mostly discussing the formulations in drafted English-language documents.

    Especially text with english contractions is likely to be left untranslated,
    which can lead the system to learn that sentences with contractions
    can be passed through.

    Do NOT apply this filter to the English side of a corpus.
    """

    def __init__(self, reverse=False):
        idx = 1 if reverse else 0
        contractions = [line.split('\t')[idx]
                        for line in read_lang_file('contractions', 'en')]
        expressions = (r'\b{}\b'.format(cont.replace("'", " ?' ?"))
                       for cont in contractions)
        super().__init__(expressions, ignore_case=True)


class FilterUntranslatedContractions(ParallelFilter):
    def __init__(self, logfile=None):
        # No filter on source side. Only apply to target side.
        filters = [NoFilter(), FilterContractions()]
        super().__init__(filters, logfile=logfile)


#class FilterReferences(object):
#    """ References to sections of some documents """
#    def __call__(self, line, side_fobjs=None):
#        """Returns True if the line should be filtered out"""
#        # if several of the following are present:
#        # several types of brackets: () and [], puctuation / -
#        # large number > 5 tokens containing numbers
#        # large number > 5 commas
