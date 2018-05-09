from .core import RegexSubstitution

class LeadingHyphen(RegexSubstitution):
    def __init__(self, **kwargs):
        super().__init__([(r'^ ?- ?', '')], **kwargs)
