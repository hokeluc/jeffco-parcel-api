from pyproj import Transformer


def coords_to_lat_long(x: int, y: int):
    transformer = Transformer.from_crs("EPSG:2232", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    print(lat, lon)
    return lat, lon

# coords_to_lat_long(3063151, 1689004)