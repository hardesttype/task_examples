import numpy as np
import pandas as pd


def TOPSIS(
        data: pd.DataFrame,
        isNeg: list = None,
        weights: list = None) -> pd.Series:
    """ Returns pd.Series of values computed according to TOPSIS method;
    data: Decision matrix;
    isNeg: List of str or int - marks the criteria with decreasing scale of preferences;
    weights: Weights of the criteria. Equal weights are by default;
    return: Resulting pd.Series; """
    # Normalization
    norma = np.linalg.norm(data, axis=0)
    norma[norma == 0] = 1  # if there is a zero vector
    data = data / norma
    # Multiplication by weights
    if weights is not None:
        data = weights * data
    else:
        data = (np.ones(len(data.columns)) * (1 / len(data.columns))) * data
    # Ideal and anti-ideal solutions
    isPos = data.columns.drop(isNeg)
    A_minus = np.min(data.loc[:, isPos], axis=0).append(
        np.max(data.loc[:, isNeg], axis=0))[data.columns]
    A_plus = np.max(data.loc[:, isPos], axis=0).append(
        np.min(data.loc[:, isNeg], axis=0))[data.columns]
    # Distance between j and ideal\anti-ideal solution
    d_minus = np.sqrt(((data - A_minus) ** 2).sum(axis=1))
    d_plus = np.sqrt(((data - A_plus) ** 2).sum(axis=1))
    # Relative distance to ideal solution of all alternatives
    R = d_plus + d_minus 
    R[R == 0] = 1  # if sum of d_plus and d_minus gives 0
    data = d_minus / R
    # Result sorted by descending of values
    return data.sort_values(ascending=False)