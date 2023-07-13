from psycopg2.extensions import adapt, register_adapter, AsIs


class Point(object):

    def __init__(self, x, y):
        self.x = x
        self.y = y


def adapt_point(point):
    x = adapt(point.x)
    y = adapt(point.y)
    return AsIs("'(%s, %s)'" % (x, y))


register_adapter(Point, adapt_point)

