from .recipe import RecipeFile

class Step(object):
    def __init__(self, inputs, outputs):
        if isinstance(inputs, RecipeFile):
            self.inputs = (inputs,)
        else:
            self.inputs = tuple(inputs.values())
        if isinstance(outputs, RecipeFile):
            self.outputs = (outputs,)
        else:
            self.outputs = tuple(outputs.values())

    def make(self, conf, cli_args=None):
        raise NotImplementedError()
