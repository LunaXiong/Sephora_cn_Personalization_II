from abc import abstractmethod


class Suggester:
    """
    Abstract class for suggester
    """
    def __init__(self):
        pass

    @abstractmethod
    def suggest(self, kw):
        pass


class HotItemSuggest(Suggester):

    def suggest(self, kw):
        pass
