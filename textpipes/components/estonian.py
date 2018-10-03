from .filtering import Filter

ACCENT_NOISE = set('/¥°ÇçÐÑÕÖàáâãäêîïòØÙùÎ')

class FilterAccentNoise(Filter):
    """Noise sequences consisting of particular characters"""
    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        tot = 0
        hits = 0
        for char in line:
            if char == ' ':
                continue
            if char in ACCENT_NOISE:
                hits += 1
            tot += 1
        if tot < 6:
            return False
        return hits / tot > .4

