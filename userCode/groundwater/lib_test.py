from userCode.groundwater.lib import generate_circle_polygon


def test_generated_circle_is_geojson():
    # make sure it conforms to geojson; it is a bit hard to test the
    # implementation of the projection since to do that would be to reimplement
    # the algorithm/geopy library
    # output should be primarily checked visually on a map to make sure
    # the radius matches the est_horizontal_error property of the data

    polygon = generate_circle_polygon(
        center_lat=37.7749, center_lon=-122.4194, radius_feet=1000, num_points=12
    )

    assert polygon
    assert len(polygon) == 1  # Should be a list of one polygon
    for item in polygon[0]:
        assert isinstance(item, tuple) and len(item) == 2
        assert isinstance(item[0], float)
        assert isinstance(item[1], float)
