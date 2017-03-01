import re

class PipeComponent(object):
    pass

class MonoPipeComponent(PipeComponent):
    pass

class ParellelPipeComponent(PipeComponent):
    pass

class SingleCellComponent(MonoPipeComponent):
    def apply(stream):
        for val in stream:
            yield self.single_cell(val)

    def single_cell(self, val):
        raise NotImplementedError()

class ForEach(ParellelPipeComponent):
    """Wraps a SingleCellComponent for use in a ParellelPipe.

    The operation will be applied to each parallel stream.
    The operation MUST NOT filter out any lines.
    """
    pass

class RegexSubstitution(SingleCellComponent):
    """Arbitrary regular expression substitutions"""
    def __init__(self, expressions):
        super().__init__()
        self.expressions = [(re.compile(exp, flags=re.UNICODE), repl)
                            for (exp, repl) in expressions]

    def single_cell(self, val):
        for (exp, repl) in self.expressions:
            val = exp.sub(repl, val)
        return val

