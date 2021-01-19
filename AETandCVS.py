import pandas as pd
import numpy as np

# cluster Path
dfTrades = pd.read_csv("/nobackup4/murrell/RohitData/CVSandAetnaTrades.csv")

# Drop Sym_Suffix(empty col)
dfTrades = dfTrades.drop(columns=['SYM_SUFFIX'])

# combine date and trade timestamp
dfTrades['Datetime'] = pd.to_datetime(dfTrades["DATE"].astype(str) + " " + dfTrades["TIME_M"].astype(str))

# No longer need separate date and time cols
dfTrades = dfTrades.drop(columns=['DATE', 'TIME_M'])

# Index by datetime
dfTrades = dfTrades.set_index('Datetime')

# Group by ticker
dfAETTrades = dfTrades[dfTrades["SYM_ROOT"] == "AET"]
dfCVSTrades = dfTrades[dfTrades["SYM_ROOT"] == "CVS"]

# Group by date(Dec 4 1st trading day after announcement)
groupedDec4AET = dfAETTrades[dfAETTrades.index.date.astype(str) == '2017-12-04']
groupedDec4CVS = dfCVSTrades[dfCVSTrades.index.date.astype(str) == '2017-12-04']

# Dec 5 2nd day after announcement
groupedDec5AET = dfAETTrades[dfAETTrades.index.date.astype(str) == '2017-12-05']
groupedDec5CVS = dfCVSTrades[dfCVSTrades.index.date.astype(str) == '2017-12-05']

# Dec 1 Last trading day before announcement
groupedDec1AET = dfAETTrades[dfAETTrades.index.date.astype(str) == '2017-12-01']
groupedDec1CVS = dfCVSTrades[dfCVSTrades.index.date.astype(str) == '2017-12-01']

# Convert to numpy arrays for speed
AETDec4Times = groupedDec4AET.index.to_numpy()
CVSDec4Times = groupedDec4CVS.index.to_numpy()

# Convert to numpy arrays for speed
AETDec5Times = groupedDec5AET.index.to_numpy()
CVSDec5Times = groupedDec5CVS.index.to_numpy()

# Convert to numpy arrays for speed
AETDec1Times = groupedDec1AET.index.to_numpy()
CVSDec1Times = groupedDec1CVS.index.to_numpy()

# Will store cross trade data for Dec 4
crossTradesDec4 = {}

# Will store cross trade data for Dec 5
crossTradesDec5 = {}

# Will store cross trade data for Dec 1
crossTradesDec1 = {}


def countCrossTrades(times1, times2, data, delta):
    totalCross = 0

    for time in times1:
        timeOffset = time + np.timedelta64(delta, 'ms')
        numCross = (times2 == timeOffset).sum()
        totalCross += numCross

    data[delta] = totalCross


for i in range(-50, 51):
    countCrossTrades(AETDec4Times, CVSDec4Times, crossTradesDec4, i)
    countCrossTrades(AETDec5Times, CVSDec5Times, crossTradesDec5, i)
    countCrossTrades(AETDec1Times, CVSDec1Times, crossTradesDec1, i)

try:
    crossTradeDf1 = pd.DataFrame(crossTradesDec4, index=[0])
    crossTradeDf1.to_csv('/nobackup4/murrell/RohitData/CVSCrossAETDec4ms.csv', index=False)

    crossTradeDf2 = pd.DataFrame(crossTradesDec5, index=[0])
    crossTradeDf2.to_csv('/nobackup4/murrell/RohitData/CVSCrossAETDec5ms.csv', index=False)

    crossTradeDf3 = pd.DataFrame(crossTradesDec1, index=[0])
    crossTradeDf3.to_csv('/nobackup4/murrell/RohitData/CVSCrossAETDec1ms.csv', index=False)

except:
    print("Error in df constructor. Values from data:")
    print("December 4")
    print(crossTradesDec4)

    print("December 5")
    print(crossTradesDec5)

    print("December 1")
    print(crossTradesDec1)

