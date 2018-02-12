from .core import RegexSubstitution
from .filtering import FilterRegex

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


class ForceTokenizeUrl(RegexSubstitution):
    """ add space in front of urls, if missing """
    def __init__(self, **kwargs):
        super().__init__([(r'([^ ])(https?://)', r'\1 \2')], **kwargs)
