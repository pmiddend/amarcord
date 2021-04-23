import logging

import numpy as np

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction

logger = logging.getLogger(__name__)


class KaraboStatistics:
    def __init__(self) -> None:
        pass

    @staticmethod
    def call(method, data, **kwargs):

        if method == KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN:
            return KaraboStatistics.arithmetic_mean(data, **kwargs)

        if method == KaraboAttributoAction.COMPUTE_STANDARD_DEVIATION:
            return KaraboStatistics.standard_deviation(data, **kwargs)

        raise KeyError(f"Method {method} not implemented")

    @staticmethod
    # pylint: disable=unused-argument
    def arithmetic_mean(data, axis=0, source=None, key=None, **kwargs):
        try:
            return np.mean(data, axis=axis)
        except TypeError as err:
            logger.warning(
                "{}//{}: Can not compute arithmetic mean on {}:type={} along axis={}".format(
                    source, key, data, type(data), axis
                )
            )

            raise TypeError(err)

    @staticmethod
    # pylint: disable=unused-argument
    def standard_deviation(data, axis=0, source=None, key=None, **kwargs):
        try:
            # noinspection PyArgumentList
            return np.std(data, axis=axis, ddof=1)
        except TypeError as err:
            logger.warning(
                "{}//{}: Can not compute standard deviation on {}:type={} along axis={}".format(
                    source, key, data, type(data), axis
                )
            )

            raise TypeError(err)
