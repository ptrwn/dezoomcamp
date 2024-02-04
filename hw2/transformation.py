if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    '''
    Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    '''

    rename_cols = {
        "VendorID": "vendor_id",
        "RatecodeID": "ratecode_id", 
        "PULocationID": "pu_location_id", 
        "DOLocationID": "do_locaiton_id"
    }

    data = data.rename(columns=rename_cols)
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
    data = data.loc[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)]

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert output[output["passenger_count"]==0].shape[0] == 0, 'there are rides with 0 passengers'
    assert output[output["trip_distance"]==0].shape[0] == 0, 'there are rides with 0 distances'
    assert "vendor_id" in output.columns, 'vendor_id is missing'

