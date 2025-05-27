from geopy.distance import distance


def generate_circle_polygon(
    center_lat: float, center_lon: float, radius_feet: float, num_points=12
) -> list[list[tuple[float, float]]]:
    """
    Generate an approximate circular polygon around a lat/lon point.

    Parameters:
        center_lat (float): Latitude of the center point.
        center_lon (float): Longitude of the center point.
        radius_feet (float): Radius of the circle in feet.
        num_points (int): Number of points to approximate the circle.

    Returns:
        List than can be used for geojson for a circular polygon around the center point.
    """
    radius_meters = radius_feet * 0.3048
    circle_points: list[tuple[float, float]] = []

    degrees_in_a_circle = 360
    degrees_per_point = degrees_in_a_circle / num_points
    for angle in range(0, degrees_in_a_circle, int(degrees_per_point)):
        point = distance(meters=radius_meters).destination(
            (center_lat, center_lon), bearing=angle
        )
        circle_points.append((point.longitude, point.latitude))  # (lon, lat)

    # Ensure the polygon is closed by repeating the first point
    circle_points.append(circle_points[0])

    # wrap it in a list to match the expected GeoJSON format for polygon
    # namely, a list of lists of coordinates
    return [circle_points]
