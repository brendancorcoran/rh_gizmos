import pandas as pd
import statsmodels.api as sm

def calculate_loess(df_party:pd.DataFrame, score_col:str, frac:float):
    df_party = df_party.sort_values('published_at')

    # Filter out neutral tweets
    non_neutral_df = df_party.query(f"abs({score_col}) > 0").copy()

    if len(non_neutral_df) < 20:
        frac = 1. / 2

    score_loess = sm.nonparametric.lowess(non_neutral_df[score_col], non_neutral_df['published_at'], frac=frac)[:, 1]
    non_neutral_df.loc[:, f'{score_col}_loess'] = score_loess
    return non_neutral_df
