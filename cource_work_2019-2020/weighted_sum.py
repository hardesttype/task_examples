import numpy as np
import pandas as pd

def weighted_sum(
        data: pd.DataFrame,
        isNeg: list = None,
        weights: list = None,
        scale: str = 'min_max') -> pd.Series:
    """ Returns pd.Series of values computed by the weighted sum method;
    data: Decision matrix;
    isNeg: List of str or int - marks the criteria with decreasing scale of preferences;
    weights: Weights of the criteria. Equal weights are by default;
    scale: Normalization method. 'min_max' by default: (x - min(x)) / (max(x) - min(x));
    return: Resulting pd.Series; """
    # Normalization
    if scale == 'min_max':
        R = data.max() - data.min(); R[R == 0] = 1  # avoid div by zero
        data = (data - data.min()) / R
    elif scale == 'max':
        data_max = np.abs(data).max(); data_max[data_max == 0] = 1  # avoid div by zero
        data = data / data_max
    elif scale == 'normal_distr':
        mean = data.mean()
        std = data.std(); std[std == 0] = 1  # avoid div by zero
        data = (data - mean) / std
    # Multiplication by weights
    if weights is not None:
        data = weights * data
    else:
        data = (np.ones(len(data.columns)) * (1 / len(data.columns))) * data
    # Negative preference scale criteria make negative
    if isNeg is not None:
        data[isNeg] = data[isNeg] * (-1)
    # Summation and result by descending of values
    return data.sum(axis=1).sort_values(ascending=False)