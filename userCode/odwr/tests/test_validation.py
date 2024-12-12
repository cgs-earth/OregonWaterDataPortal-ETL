from ..dag import station_metadata

def test_validation():
    """Check to make sure that the pydantic validation works on the json response"""
    station_metadata()