import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def validate_iris(df, *args, **kwargs):
    """Check no missing values and exactly 3 target classes."""
    missing = df.isnull().sum().sum()
    n_classes = df['target'].nunique()
    print(f'Missing values: {missing}')
    print(f'Number of classes: {n_classes}')
    return df

@test
def test_no_missing_and_three_classes(output, *args):
    assert output.isnull().sum().sum() == 0, 'Dataset has missing values'
    assert output['target'].nunique() == 3, 'Expected exactly 3 classes'
