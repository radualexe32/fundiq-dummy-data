import numpy as np
import polars as pl


class ReturnIndicators:
    def __init__(self, length=100):
        self.length = length

    def twr(self):
        pass

    def mwr(self):
        pass

    def twt(self):
        pass

    def mwt(self):
        pass


class VolIndicators:
    def __init__(self, length=100):
        self.length = length

    def mdd(self):
        pass

    def mhw(self):
        pass

    def psd(self):
        pass

    def dsd(self):
        pass

    def usd(self):
        pass

    def rtd(self):
        pass

    def rdd(self):
        pass

    def rud(self):
        pass


class MptIndicators:
    def __init__(self, length=100):
        self.length = length

    def sor(self):
        pass

    def shr(self):
        pass

    def rrr(self):
        pass

    def ifr(self):
        pass

    def trr(self):
        pass

    def alf(self):
        pass

    def bet(self):
        pass
