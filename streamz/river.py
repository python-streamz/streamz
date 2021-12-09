from . import Stream


# TODO: most river classes support batches, e.g., learn_many, more efficiently


class RiverTransform(Stream):
    """Pass data through one or more River transforms"""

    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def update(self, x, who=None, metadata=None):
        out = self.model.transform_one(*x)
        self.emit(out)


class RiverTrain(Stream):

    def __init__(self, model, metric=None, pass_model=False, **kwargs):
        """

        If metric and pass_model are both defaults, this is effectively
        a sink.

        :param model: river model or pipeline
        :param metric: river metric
            If given, it is emitted on every sample
        :param pass_model: bool
            If True, the (updated) model if emitted for each sample
        """
        super().__init__(**kwargs)
        self.model = model
        if pass_model and metric is not None:
            raise TypeError
        self.pass_model = pass_model
        self.metric = metric

    def update(self, x, who=None, metadata=None):
        """
        :param x: tuple
            (x, [y[, w]) floats for single sample. Include
        """
        self.model.learn_one(*x)
        if self.metric:
            yp = self.model.predict_one(x[0])
            weights = x[2] if len(x) > 1 else 1.0
            return self._emit(self.metric.update(x[1], yp, weights).get(), metadata=metadata)
        if self.pass_model:
            return self._emit(self.model, metadata=metadata)


class RiverPredict(Stream):

    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def update(self, x, who=None, metadata=None):
        out = self.model.predict_one(x)
        return self._emit(out, metadata=metadata)
