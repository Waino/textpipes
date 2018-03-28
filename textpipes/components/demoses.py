from .core import RegexSubstitution


class AmpDoubleEntity(RegexSubstitution):
    def __init__(self, **kwargs):
        super().__init__([(r'&amp; amp ;', '&')], **kwargs)

class ZeroWidthSpace(RegexSubstitution):
    def __init__(self, **kwargs):
        super().__init__([('\u200b', ' ')], **kwargs)
