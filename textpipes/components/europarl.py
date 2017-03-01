from .core import RegexSubstitution

class RemoveLanguageTags(RegexSubstitution):
    def __init__(self):
        super().__init__([(r'^([\.-] )*\([A-Z][A-Z]\) ', '')])
