import logging

logger = logging.getLogger(__name__)

try:
    import nltk
    #from nltk.tokenize import moses
except ImportError:
    logger.warning('Unable to load nltk.')
    logger.warning('You will not be able to use (De)Tokenize.')

# nltk.download('perluniprops')
# nltk.download('nonbreaking_prefixes')


#nltk.tokenize.casual.reduce_lengthening(text)
#Replace repeated character sequences of length 3 or greater with sequences of length 3.

