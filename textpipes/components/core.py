import re

class PipeComponent(object):
    def __init__(self, side_inputs=None, side_outputs=None):
        self.side_inputs = side_inputs if side_inputs is not None else ()
        self.side_outputs = side_outputs if side_outputs is not None else ()

    def pre_make(self, side_fobjs):
        """Called before __call__ (or all the single_cell calls)"""
        pass

    def post_make(self, side_fobjs):
        """Called after __call__ (or all the single_cell calls)"""
        pass

class MonoPipeComponent(PipeComponent):
    pass

class ParallelPipeComponent(PipeComponent):
    pass

class SingleCellComponent(MonoPipeComponent):
    def __call__(self, stream, side_fobjs=None):
        for line in stream:
            yield self.single_cell(line, side_fobjs=side_fobjs)

    def single_cell(self, line, side_fobjs=None):
        raise NotImplementedError()

class ForEach(ParallelPipeComponent):
    """Wraps a SingleCellComponent for use in a ParallelPipe.

    The operation will be applied to each parallel stream.
    The operation MUST NOT filter out any lines.
    """
    def __init__(self, mono_component):
        self.mono_component = mono_component

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            yield tuple(self.mono_component.single_cell(
                            line, side_fobjs=side_fobjs)
                        for line in tpl)

    @property
    def side_inputs(self):
        return self.mono_component.side_inputs

    @property
    def side_outputs(self):
        return self.mono_component.side_outputs

class PerColumn(ParallelPipeComponent):
    """Wraps multiple SingleCellComponents for use in a ParallelPipe,
    such that each column of the parallel pipe gets a different component."""
    def __init__(self, components):
        self.components = [component if component is not None
                           else IdentityComponent()
                           for component in components]

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            assert len(tpl) == len(self.components)
            yield tuple(component.single_cell(line, side_fobjs=side_fobjs)
                        for (component, line) in zip(self.components, tpl))

    @property
    def side_inputs(self):
        return tuple(set(inp for component in self.components
                         for inp in component.side_inputs))

    @property
    def side_outputs(self):
        return tuple(set(inp for component in self.components
                         for inp in component.side_outputs))

class IdentityComponent(SingleCellComponent):
    def single_cell(self, line, side_fobjs=None):
        return line

class RegexSubstitution(SingleCellComponent):
    """Arbitrary regular expression substitutions"""
    def __init__(self, expressions):
        super().__init__()
        self.expressions = [(re.compile(exp, flags=re.UNICODE), repl)
                            for (exp, repl) in expressions]

    def single_cell(self, line, side_fobjs=None):
        for (exp, repl) in self.expressions:
            line = exp.sub(repl, line)
        return line
