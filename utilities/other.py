

def curve_drop(distance_miles: float) -> float:
    # curnve drop in inches
    drop_inches = (distance_miles ** 2) * 8 / 12
    return drop_inches