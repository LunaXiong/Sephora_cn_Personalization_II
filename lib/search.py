from abc import abstractmethod


class Searcher:
    def __init__(self):
        pass

    @abstractmethod
    def search(self, kw):
        pass


