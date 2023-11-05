"""
Test goes here

"""
import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True


def test_load_data(spark):
    df = load_data(spark)
    assert df is not None


def test_describe(spark):
    df = load_data(spark)
    result = describe(df)
    assert result is None


def test_query(spark):
    df = load_data(spark)
    result = query(
        spark,
        df,
        "SELECT * FROM BIRTH8000 WHERE Births > 8000",
        "BIRTH8000"
        # call the "Spark" session
        # Call the dataframe called "df"
        # The new dataframe is called "BIRTH8000"
        # query the data based on the new dataframe
    )
    assert result is None


def test_example_transform(spark):
    df = load_data(spark)
    result = example_transform(df)
    assert result is None


if __name__ == "__main__":
    test_extract()
    test_load_data(spark)
    test_describe(spark)
    test_query(spark)
    test_example_transform(spark)
