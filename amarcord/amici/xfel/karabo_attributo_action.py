from enum import Enum


class KaraboAttributoAction(Enum):
    COMPUTE_ARITHMETIC_MEAN = "compute_arithmetic_mean"
    COMPUTE_STANDARD_DEVIATION = "compute_standard_deviation"
    CHECK_IF_CONSTANT = "check_if_constant"
    STORE_LAST = "store_last"
