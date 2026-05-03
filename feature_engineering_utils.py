"""
Feature Engineering Utilities for Safe Expression Evaluation
"""

import numpy as np
import pandas as pd
import math
import statistics
import datetime
import re
import string
from typing import Dict, Any
import ast
import operator



def create_safe_environment(df: pd.DataFrame, exec_globals: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create a safe evaluation environment for feature engineering expressions.
    
    Parameters:
    -----------
    df : pd.DataFrame
        The dataframe containing the data
    exec_globals : dict, optional
        Globals from executed imports
        
    Returns:
    --------
    dict : Safe environment for eval()
    """
    if exec_globals is None:
        exec_globals = {}
    
    safe_env = {
        # === CORE LIBRARIES ===
        'np': exec_globals.get('np', np),
        'pd': exec_globals.get('pd', pd),
        'math': exec_globals.get('math', math),
        
        # === STATISTICS ===
        'statistics': exec_globals.get('statistics', statistics),
        
        # === DATE/TIME ===
        'datetime': exec_globals.get('datetime', datetime),
        'timedelta': exec_globals.get('timedelta', datetime.timedelta),
        
        # === STRING/TEXT ===
        're': exec_globals.get('re', re),
        'string': exec_globals.get('string', string),
        
        # === DATAFRAME ===
        'df': df,
        
        # === BUILT-IN FUNCTIONS ===
        'abs': abs, 'round': round, 'min': min, 'max': max,
        'sum': sum, 'len': len, 'pow': pow, 'divmod': divmod,
        'sorted': sorted, 'enumerate': enumerate, 'zip': zip,
        'range': range, 'map': map, 'filter': filter,
        
        # === TYPE CONVERSION ===
        'int': int, 'float': float, 'str': str, 'bool': bool,
        'list': list, 'tuple': tuple, 'dict': dict, 'set': set,
        
        # === MATH CONSTANTS ===
        'pi': math.pi, 'e': math.e, 'inf': float('inf'), 
        'nan': float('nan'), 'tau': math.tau,
        
        # === SPECIAL VALUES ===
        'None': None, 'True': True, 'False': False,
        'NaN': np.nan, 'Inf': np.inf, '-Inf': -np.inf,
    }
    
    # Add dataframe columns as variables (with underscores for spaces)
    for col in df.columns:
        var_name = col.replace(' ', '_').replace('(', '').replace(')', '')
        safe_env[var_name] = df[col]
        # Also keep original column name if it's a valid Python identifier
        if col.replace(' ', '_').isidentifier():
            safe_env[col.replace(' ', '_')] = df[col]
    
    # Add common numpy functions
    numpy_funcs = ['mean', 'median', 'std', 'var', 'percentile', 'quantile',
                   'corrcoef', 'cov', 'unique', 'where', 'select', 'clip',
                   'digitize', 'interp', 'expm1', 'log1p', 'log2',
                   'sign', 'isnan', 'isinf', 'isfinite', 'isclose',
                   'nan_to_num', 'nanmean', 'nanstd', 'nanvar',
                   'array', 'arange', 'linspace', 'zeros', 'ones']
    
    for func in numpy_funcs:
        if hasattr(np, func):
            safe_env[func] = getattr(np, func)
    
    # Add common pandas functions
    pandas_funcs = ['cut', 'qcut', 'get_dummies', 'to_datetime', 
                    'to_numeric', 'concat', 'merge', 'date_range']
    
    for func in pandas_funcs:
        if hasattr(pd, func):
            safe_env[func] = getattr(pd, func)
    
    # Add math functions
    math_funcs = ['sqrt', 'log', 'log10', 'exp', 'sin', 'cos', 'tan',
                  'asin', 'acos', 'atan', 'sinh', 'cosh', 'tanh',
                  'degrees', 'radians', 'ceil', 'floor', 'trunc',
                  'fabs', 'factorial', 'gcd', 'isclose']
    
    for func in math_funcs:
        if hasattr(math, func):
            safe_env[func] = getattr(math, func)
    
    return safe_env


def evaluate_feature_expression(expression: str, df: pd.DataFrame, 
                                exec_globals: Dict[str, Any] = None) -> pd.Series:
    """
    Safely evaluate a feature engineering expression.
    
    Parameters:
    -----------
    expression : str
        The expression to evaluate
    df : pd.DataFrame
        The dataframe
    exec_globals : dict, optional
        Globals from executed imports
        
    Returns:
    --------
    pd.Series : Result of the expression
    """
    safe_env = create_safe_environment(df, exec_globals)
    
    try:
        result = eval(expression, {"__builtins__": {}}, safe_env)
        
        # Convert result to Series if needed
        if isinstance(result, pd.Series):
            return result
        elif isinstance(result, pd.DataFrame) and len(result.columns) == 1:
            return result.iloc[:, 0]
        elif isinstance(result, (int, float, str, bool)) or pd.isna(result):
            return pd.Series([result] * len(df), index=df.index)
        else:
            try:
                return pd.Series(result, index=df.index)
            except:
                raise ValueError(f"Cannot convert result of type {type(result)} to Series")
                
    except Exception as e:
        raise ValueError(f"Error evaluating expression: {str(e)}")


def get_default_imports() -> str:
    """
    Return default import statements for feature engineering.
    
    Returns:
    --------
    str : Default import statements
    """
    return """import numpy as np
        import pandas as pd
        import math
        import statistics
        import datetime
        from datetime import timedelta
        import re
        import string"""



def safe_evaluate(expr, df):
    SAFE_OPERATORS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.Mod: operator.mod,
        ast.USub: operator.neg,
    }

    SAFE_COMPARISONS = {
        ast.Gt: operator.gt,
        ast.Lt: operator.lt,
        ast.GtE: operator.ge,
        ast.LtE: operator.le,
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
    }

    SAFE_BOOL_OPS = {
        ast.And: np.logical_and,
        ast.Or: np.logical_or,
    }

    SAFE_FUNCTIONS = {
        "abs": np.abs,
        "sqrt": np.sqrt,
        "log": np.log,
        "exp": np.exp,
        "sin": np.sin,
        "cos": np.cos,
        "tan": np.tan,
        "where": np.where,
    }

    def _eval(node):

        if isinstance(node, ast.BinOp):
            return SAFE_OPERATORS[type(node.op)](
                _eval(node.left), _eval(node.right)
            )

        elif isinstance(node, ast.UnaryOp):
            return SAFE_OPERATORS[type(node.op)](_eval(node.operand))

        elif isinstance(node, ast.Constant):
            return node.value

        elif isinstance(node, ast.Name):
            if node.id in df.columns:
                return df[node.id]
            else:
                raise ValueError(f"Unknown column: {node.id}")

        elif isinstance(node, ast.Compare):
            left = _eval(node.left)
            for op, comparator in zip(node.ops, node.comparators):
                right = _eval(comparator)
                left = SAFE_COMPARISONS[type(op)](left, right)
            return left

        elif isinstance(node, ast.BoolOp):
            values = [_eval(v) for v in node.values]
            result = values[0]
            for v in values[1:]:
                result = SAFE_BOOL_OPS[type(node.op)](result, v)
            return result

        elif isinstance(node, ast.Call):
            func_name = node.func.id
            if func_name not in SAFE_FUNCTIONS:
                raise ValueError(f"Function '{func_name}' is not allowed")

            args = [_eval(arg) for arg in node.args]
            return SAFE_FUNCTIONS[func_name](*args)

        else:
            raise ValueError(f"Unsupported expression: {ast.dump(node)}")

    tree = ast.parse(expr, mode="eval")
    return _eval(tree.body)