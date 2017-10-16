from .core import RegexSubstitution

class RemoveLanguageTags(RegexSubstitution):
    def __init__(self):
        super().__init__([(r'^([\.-] )*\([A-Za-z][A-Za-z]\) ', '')])


class FilterContractions(FilterRegex):
    """
    Europarl contains parts that have been left untranslated,
    mostly discussing the formulations in drafted English-language documents.

    Especially text with english contractions is likely to be left untranslated,
    which can lead the system to learn that sentences with contractions
    can be passed through.

    Do NOT apply this filter to the English side of a corpus.
    """

    def __init__(self):
        contractions = (
            "ain't",
            "can't",
            "couldn't",
            "didn't",
            "doesn't",
            "don't",
            "haven't",
            "he's",
            "here's",
            "i'd",
            "i'll",
            "i'm",
            "i've",
            "isn't",
            "it's",
            "let's",
            "she's",
            "that's",
            "there's",
            "they're",
            "wasn't",
            "we're",
            "we've",
            "what's",
            "won't",
            "wouldn't",
            "you'll",
            "you're",
            "you've",)
        #
        expressions = (r'\b{}\b'.format(cont.replace("'", " ?' ?"))
                       for cont in contractions)
        super().__init__(expressions, ignore_case=True)


class FilterUntranslatedContractions(ParallelFilter):
    def __init__(self, logfile=None):
        # No filter on source side. Only apply to target side.
        filters = [NoFilter(), FilterContractions()]
        super().__init__(filters, logfile=logfile)
