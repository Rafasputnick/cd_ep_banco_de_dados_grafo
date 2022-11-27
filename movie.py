class Movie:
    def __init__(self, title: str) -> None:
        self.title = title
        self.related_points = 1

    def multiplyPoints(self, multiplier: float):
        self.related_points *= multiplier