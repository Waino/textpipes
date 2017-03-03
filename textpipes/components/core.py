import re

class PipeComponent(object):
    pass

class MonoPipeComponent(PipeComponent):
    pass

class ParallelPipeComponent(PipeComponent):
    pass

class SingleCellComponent(MonoPipeComponent):
    def __call__(self, stream):
        for line in stream:
            yield self.single_cell(line)

    def single_cell(self, line):
        raise NotImplementedError()

class ForEach(ParallelPipeComponent):
    """Wraps a SingleCellComponent for use in a ParallelPipe.

    The operation will be applied to each parallel stream.
    The operation MUST NOT filter out any lines.
    """
    def __init__(self, mono_component):
        self.mono_component = mono_component

    def __call__(self, stream):
        for tpl in stream:
            yield tuple(self.mono_component.single_cell(line)
                        for line in tpl)


class RegexSubstitution(SingleCellComponent):
    """Arbitrary regular expression substitutions"""
    def __init__(self, expressions):
        super().__init__()
        self.expressions = [(re.compile(exp, flags=re.UNICODE), repl)
                            for (exp, repl) in expressions]

    def single_cell(self, line):
        for (exp, repl) in self.expressions:
            line = exp.sub(repl, line)
        return line
