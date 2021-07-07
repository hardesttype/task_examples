import numpy as np
import pandas as pd


def intervalTOPSIS(
        lowerBounds: pd.DataFrame,
        upperBounds: pd.DataFrame,
        isNeg: list = None,
        weights_lower: list = None,
        weights_upper: list = None,
        norm: str = 'min_max', 
        metric: str = 'haus') -> pd.Series:
    """ Returns pd.Series of values computed according to interval TOPSIS method;
    lowerBounds: Decision matrix of lower bounds;
    upperBounds: Decision matrix of upper bounds;
    isNeg: List of str or int - marks the criteria with decreasing scale of preferences;
    weights_lower (weights_upper): Weights of lower (upper) bounds of the criteria. Equal weights are by default;
    return: Resulting pd.Series; """
    # Normalization
    if norm == 'min_max':
        mins = lowerBounds.agg('min')
        h = upperBounds.agg('max') - mins; h[h == 0] = 1
        lowerBounds = (lowerBounds - mins) / h
        upperBounds = (upperBounds - mins) / h
    elif norm == 'euclid':
        norm = np.sqrt((lowerBounds ** 2 + upperBounds ** 2).sum(axis = 0))
        norm[norm == 0] = 1
        lowerBounds = lowerBounds / norm
        upperBounds = upperBounds / norm
    # Multiplication by weights
    if weights is not None:
        lowerBounds = weights_lower * lowerBounds
        upperBounds = weights_upper * upperBounds
    else:
        lowerBounds = (np.ones(len(lowerBounds.columns)) * (1 / len(lowerBounds.columns))) * lowerBounds
        upperBounds = (np.ones(len(lowerBounds.columns)) * (1 / len(lowerBounds.columns))) * upperBounds
    # Ideal and anti-ideal solutions
    isPos = lowerBounds.columns.drop(isNeg)
    A_minus = np.min(lowerBounds.loc[:, isPos], axis=0).append(
        np.max(upperBounds.loc[:, isNeg], axis=0))[upperBounds.columns]
    A_plus = np.max(upperBounds.loc[:, isPos], axis=0).append(
        np.min(lowerBounds.loc[:, isNeg], axis=0))[upperBounds.columns]
    # Distance between j and ideal\anti-ideal solution
    if metric == 'haus':
        d_minus = (1 / len(upperBounds.columns)) * (np.abs(lowerBounds - A_minus).
                    combine(np.abs(upperBounds - A_minus),
                            lambda x, y: x.
                            combine(y, lambda n, m : (n, m))).
                    applymap(max).sum(axis = 1))
        d_plus = (1 / len(upperBounds.columns)) * (np.abs(lowerBounds - A_plus).
                   combine(np.abs(upperBounds - A_plus),
                           lambda x, y: x.
                           combine(y, lambda n, m : (n, m))).
                   applymap(max).sum(axis = 1))
    elif metric == 'euclid':
        d_minus = np.sqrt(0.5 * ((upperBounds - A_minus) ** 2).
                          sum(axis = 1) + ((lowerBounds - A_minus) ** 2).
                          sum(axis = 1))
        d_plus = np.sqrt(0.5 * ((lowerBounds - A_plus) ** 2).
                          sum(axis = 1) + ((upperBounds - A_plus) ** 2).
                          sum(axis = 1))
    # Relative distance to ideal solution of all alternatives
    data = np.nan_to_num(d_minus / d_plus + d_minus, nan = 0)
    # Result sorted by descending of values
    return data.sort_values(ascending=False)
