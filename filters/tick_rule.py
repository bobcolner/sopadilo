

class TickRule:
    
    def __init__(self, price: float=None, prev_price: float=None, side: int=0, prev_side: int=0):
        self.price = price
        self.prev_price = prev_price
        self.side = side
        self.prev_side = prev_side

    def update(self, next_price: float) -> int:
        self.prev_price = self.price
        self.price = next_price
        self.prev_side = self.side
        self.side = tick_rule(self.price, self.prev_price, self.prev_side)
        return self.side


def tick_rule(price: float, prev_price: float, prev_side: int=0) -> int:
    
    try:
        diff = price - prev_price
    except:
        diff = 0.0

    if diff > 0.0:
        side = 1
    elif diff < 0.0:
        side = -1
    elif diff == 0.0:
        side = prev_side
    else:
        side = 0

    return side
