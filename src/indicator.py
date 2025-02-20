import numpy as np
import polars as pl


class ReturnIndicators:
    def __init__(self, length=100):
        self.length = length

    def twr(self):
        # Time-Weighted Returns: assume small daily returns ~0.1% on average
        return np.random.normal(loc=0.001, scale=0.01, size=self.length)

    def mwr(self):
        # Money-Weighted Returns: similar mean, but slightly more volatile
        return np.random.normal(loc=0.001, scale=0.015, size=self.length)

    def twt(self):
        # Time-Weighted Turnover (or similar): expressed as a percentage [0,1]
        return np.random.uniform(low=0, high=1, size=self.length)

    def mwt(self):
        # Money-Weighted Turnover (or similar): expressed as a percentage [0,1]
        return np.random.uniform(low=0, high=1, size=self.length)


class VolIndicators:
    def __init__(self, length=100):
        self.length = length

    def mdd(self):
        # Maximum Drawdown: typically a negative number (0% to -50% loss)
        return np.random.uniform(low=-0.5, high=0, size=self.length)

    def mhw(self):
        # Maximum High Watermark: e.g., price levels between 100 and 200
        return np.random.uniform(low=100, high=200, size=self.length)

    def psd(self):
        # Price Standard Deviation: a small positive value
        return np.random.uniform(low=0.01, high=0.05, size=self.length)

    def dsd(self):
        # Downside Standard Deviation: typically lower than overall price std
        return np.random.uniform(low=0.005, high=0.03, size=self.length)

    def usd(self):
        # Upside Standard Deviation: similar range as downside std
        return np.random.uniform(low=0.005, high=0.03, size=self.length)

    def rtd(self):
        # Example volatility metric (e.g., total deviation)
        return np.random.uniform(low=0, high=0.1, size=self.length)

    def rdd(self):
        # Another volatility metric (e.g., drawdown deviation)
        return np.random.uniform(low=0, high=0.1, size=self.length)

    def rud(self):
        # Yet another volatility metric (e.g., upside deviation)
        return np.random.uniform(low=0, high=0.1, size=self.length)


class MptIndicators:
    def __init__(self, length=100):
        self.length = length

    def sor(self):
        # Sortino Ratio: typically between 0 and 3
        return np.random.uniform(low=0, high=3, size=self.length)

    def shr(self):
        # Sharpe Ratio: typically between 0 and 3
        return np.random.uniform(low=0, high=3, size=self.length)

    def rrr(self):
        # Risk Reward Ratio: can vary widely; here assume 0 to 10
        return np.random.uniform(low=0, high=10, size=self.length)

    def ifr(self):
        # Information Ratio: may be negative or positive (e.g., -1 to 1)
        return np.random.uniform(low=-1, high=1, size=self.length)

    def trr(self):
        # Treynor Ratio: often between 0 and 3
        return np.random.uniform(low=0, high=3, size=self.length)

    def alf(self):
        # Alpha: small values, positive or negative (e.g., -5% to 5%)
        return np.random.uniform(low=-0.05, high=0.05, size=self.length)

    def bet(self):
        # Beta: typically in the range of 0.5 to 2.0
        return np.random.uniform(low=0.5, high=2.0, size=self.length)


def generate_indicators_dataframe(length=100):
    ret_ind = ReturnIndicators(length)
    vol_ind = VolIndicators(length)
    mpt_ind = MptIndicators(length)

    data = {
        "twr": ret_ind.twr(),
        "mwr": ret_ind.mwr(),
        "twt": ret_ind.twt(),
        "mwt": ret_ind.mwt(),
        "mdd": vol_ind.mdd(),
        "mhw": vol_ind.mhw(),
        "psd": vol_ind.psd(),
        "dsd": vol_ind.dsd(),
        "usd": vol_ind.usd(),
        "rtd": vol_ind.rtd(),
        "rdd": vol_ind.rdd(),
        "rud": vol_ind.rud(),
        "sor": mpt_ind.sor(),
        "shr": mpt_ind.shr(),
        "rrr": mpt_ind.rrr(),
        "ifr": mpt_ind.ifr(),
        "trr": mpt_ind.trr(),
        "alf": mpt_ind.alf(),
        "bet": mpt_ind.bet(),
    }

    return pl.DataFrame(data)
