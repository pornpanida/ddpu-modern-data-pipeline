import json

import great_expectations as gx
import pandas as pd


def _validate_data():
    columns = ["NewConfirmed", "Date"]
    df = pd.read_csv("data.csv", names=columns)

    valid_df = df[(df.NewConfirmed >= 0) & (df.NewConfirmed <= 322315)]
    dq = len(valid_df) / len(df)
    print(dq)

    assert len(valid_df) == len(df)


_validate_data()