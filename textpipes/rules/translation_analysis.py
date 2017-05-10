import collections
import re

#from ..recipe import Rule
from ..pipe import ParallelPipe
from ..components import ParallelPipeComponent, PerColumn, IdentityComponent
from ..components.tokenizer import Tokenize

class AnalyzeTranslations(ParallelPipe):
    def __init__(self, components, main_inputs, main_outputs):
        # 6 inputs:
        #   4 sgm files: source, bl, sys, (multi)ref
        #   2 mteval bleu outputs: bl, sys
        # components must take 5 or more columns
        # where 4 first are (src, bl, sys, BLEU) and rest are refs
        # FIXME: pointless to pass BLEU through components? or is it needed for something?

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='rb')
                   for inp in self.main_inputs]

        # FIXME: fully read in each input into dicts
        # FIXME: when reading in source (for order)
        # yield into stream a tuple with one or more columns from each

        stream, side_fobjs = self._make_helper(stream, conf, cli_args)

        # Drain pipeline, throwing the output away
        for tpl in stream:
            pass
        # post_make must be done after draining
        self._post_make(side_fobjs)
        # close all file objects
        for fobj in readers + list(side_fobjs.values()):
            fobj.close()
        
    #def ._post_make(self, side_fobjs):


class AnalyzeChrF(ParallelPipeComponent):
    """Perform sentence level character-F bl-refs and sys-refs.
    This should be applied before retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            pass


class ReTokenize(PerColumn):
    """Inputs are postprocessed translations.
    Analysis that prefers tokenized input should be
    preceded by this component."""
    def __init__(self, src_lang, trg_lang, n_refs=1):
        src_tokenizer = Tokenize(src_lang)
        trg_tokenizer = Tokenize(trg_lang)
        components = [src_tokenizer,
                      trg_tokenizer,        # bl
                      trg_tokenizer,        # sys
                      IdentityComponent(),  # BLEU
                     ]
        for _ in range(n_refs):
            # all refs tokenized by target tokenizer
            components.append(trg_tokenizer)
        super().__init__(components)


class AnalyzeLetteredNames(ParallelPipeComponent):
    """Counts how many tokens in the source are eligible for lettering
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            pass


class AnalyzeRepetitions(ParallelPipeComponent):
    """Counts repeated tokens in the references and translations
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            pass


class AnalyzeLength(ParallelPipeComponent):
    """Counts length in tokens and chars
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            pass
