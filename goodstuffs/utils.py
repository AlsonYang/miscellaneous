import pandas as pd



def select_reader(filename):
    if filename.endswith("csv"):
        return pd.read_csv
    else:
        return pd.read_excel

def print_null(df):
    if df.isnull().sum().values.any():
        n_row = df.shape[0]
        for col in df:
            if df[col].isnull().any():
                print('{} has {} null values: {}%'.format(
                    col, df[col].isnull().sum(), round(df[col].isnull().sum()/n_row*100,2)))
    else:
        print('There is no null in dataframe')


def bar_plot(x_series, y_series, xlabel='', ylabel='', str_format='{:.2f}', color='Blues', figsize = [5, 4]):
    import matplotlib.pyplot as plt
    import seaborn as sns
    _, ax = plt.subplots(1, 1, figsize = figsize)
    sns.barplot(y=y_series, x=x_series, 
                ax=ax, palette=sns.color_palette(color, 1))
    for side in ['top', 'right', 'left']:
        ax.spines[side].set_visible(False)
        ax.grid(axis='x', linestyle='--')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    
    for i, v in enumerate(x_series):
        ax.text(v+0.01, i+0.3, str_format.format(x_series[i]))