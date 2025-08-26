import pandas as pd
import numpy as np
from dateutil.parser import parse as dateparse

class DataService:
    def read_file(self, filename, bytes_io):
        lower = filename.lower()
        bytes_io.seek(0)
        if lower.endswith('.csv'):
            return pd.read_csv(bytes_io)
        elif lower.endswith(('.xls', '.xlsx')):
            return pd.read_excel(bytes_io)
        else:
            try:
                bytes_io.seek(0)
                return pd.read_csv(bytes_io)
            except Exception as e:
                raise e

    def get_summary(self, df: pd.DataFrame):
        num = df.select_dtypes(include=[np.number])
        describe = num.describe().to_dict()
        cat = df.select_dtypes(exclude=[np.number])
        cat_summary = {col: list(df[col].dropna().unique()[:10]) for col in cat.columns}
        return {'numeric': describe, 'categorical_sample_values': cat_summary}

    def get_columns(self, df: pd.DataFrame):
        cols = list(df.columns)
        numeric = list(df.select_dtypes(include=[np.number]).columns)
        datetime = []
        for c in df.columns:
            if df[c].dtype == 'datetime64[ns]':
                datetime.append(c)
            else:
                try:
                    sample = df[c].dropna().astype(str).iloc[:10]
                    parsed = 0
                    for v in sample:
                        try:
                            dateparse(v)
                            parsed += 1
                        except Exception:
                            continue
                    if parsed >= 3:
                        datetime.append(c)
                except Exception:
                    pass
        return {'all': cols, 'numeric': numeric, 'datetime': datetime}

    def from_records(self, records):
        return pd.DataFrame.from_records(records)

    def prepare_chart_data(self, df, x_col, y_col=None, chart_type='bar', agg='sum'):
        labels, values = [], []

        # Aggregation function mapping
        agg_map = {
            'sum': 'sum',
            'mean': 'mean',
            'count': 'count',
            'min': 'min',
            'max': 'max'
        }
        agg_func = agg_map.get(agg, 'sum')

        if chart_type in ['bar', 'line', 'radar']:
            if x_col and y_col:
                grouped = df.groupby(x_col)[y_col]
                if agg_func == 'sum':
                    res = grouped.sum()
                elif agg_func == 'mean':
                    res = grouped.mean()
                elif agg_func == 'count':
                    res = grouped.count()
                elif agg_func == 'min':
                    res = grouped.min()
                elif agg_func == 'max':
                    res = grouped.max()
                else:
                    res = grouped.sum()
                labels = [str(i) for i in res.index.tolist()]
                values = res.values.tolist()
            elif y_col:
                # If no x_col, just show values
                values = df[y_col].tolist()
                labels = [str(i) for i in range(len(values))]

        elif chart_type in ['pie', 'doughnut']:
            if x_col:
                counts = df[x_col].value_counts().nlargest(10)
                labels = counts.index.astype(str).tolist()
                values = counts.values.tolist()
            elif y_col:
                # If only numeric y_col selected
                labels = [str(i) for i in range(len(df[y_col]))]
                values = df[y_col].tolist()

        return {'labels': labels, 'values': values}
