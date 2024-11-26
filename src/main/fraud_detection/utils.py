import math


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points on the Earth's surface."""
    r = 6371  # Earth radius in kilometers
    lat_distance = math.radians(lat2 - lat1)
    lon_distance = math.radians(lon2 - lon1)
    a = (math.sin(lat_distance / 2) ** 2 + \
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
         math.sin(lon_distance / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c
    return distance

