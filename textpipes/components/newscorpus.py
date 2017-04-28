from .filtering import FilterRegex, ParallelFilter, NoFilter

class FilterIltasanomat(FilterRegex):
    """
    the news.2015.fi.shuffled.gz and news.2016.fi.shuffled.gz corpora
    contain a large number of URL crawling artefacts. Most are of type
    http://www.iltasanomat.fi/haku/
    http://www.iltasanomat.fi/henkilo/
    but also many other URLS under the same domain
    """

    def __init__(self, aggressive=True):
        if aggressive:
            expressions = ('http://www\.iltasanomat\.fi',)
        else:
            expressions = ('http://www\.iltasanomat\.fi/haku/',
                           'http://www\.iltasanomat\.fi/henkilo/',)
        super().__init__(expressions)


class FilterContractions(FilterRegex):
    """
    News and crawled text contain parts that have been left untranslated
    both intentionally and due to crawler error.
    Especially text with contractions is likely to be left untranslated,
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
