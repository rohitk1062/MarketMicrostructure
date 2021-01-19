import pandas as pd
import numpy as np

# cluster Path
dfTrades = pd.read_csv("/nobackup4/murrell/RohitData/IBMandRHTTrades.csv")

# Local path
# dfTrades = pd.read_csv("/Users/rohitk/Documents/SURE Data/IBMandRHTTrades.csv")

# Drop Sym_Suffix(empty col)
dfTrades = dfTrades.drop(columns=['SYM_SUFFIX'])

# combine date and trade timestamp
dfTrades['Datetime'] = pd.to_datetime(dfTrades["DATE"].astype(str) + " " + dfTrades["TIME_M"].astype(str))

# No longer need separate date and time cols
dfTrades = dfTrades.drop(columns=['DATE', 'TIME_M'])

# Index by datetime
dfTrades = dfTrades.set_index('Datetime')

# Group by ticker
dfRHTTrades = dfTrades[dfTrades["SYM_ROOT"] == "RHT"]
dfIBMTrades = dfTrades[dfTrades["SYM_ROOT"] == "IBM"]

# Group by date(Oct 29 1st trading day after announcement)
groupedOct29RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-29']
groupedOct29IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-29']

# Oct 30 2nd day after announcement
groupedOct30RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-30']
groupedOct30IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-30']

# Last trading day before announcement
groupedOct26RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-26']
groupedOct26IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-26']

# Convert to numpy arrays for speed
RHTOct29Times = groupedOct29RHT.index.to_numpy()
IBMOct29Times = groupedOct29IBM.index.to_numpy()

# Convert to numpy arrays for speed
RHTOct30Times = groupedOct30RHT.index.to_numpy()
IBMOct30Times = groupedOct30IBM.index.to_numpy()

# Convert to numpy arrays for speed
RHTOct26Times = groupedOct26RHT.index.to_numpy()
IBMOct26Times = groupedOct26IBM.index.to_numpy()

# Will store cross trade data for Oct 29
crossTradesOct29 = {}

# Will store cross trade data for Oct 30
crossTradesOct30 = {}

# Will store cross trade data for Oct 26
crossTradesOct26 = {}


def countCrossTrades(times1, times2, data, delta):
    totalCross = 0

    for time in times1:
        timeOffset = time + np.timedelta64(delta, 'ms')
        numCross = (times2 == timeOffset).sum()
        totalCross += numCross

    data[delta] = totalCross


for i in range(-50, 51):
    countCrossTrades(RHTOct29Times, IBMOct29Times, crossTradesOct29, i)
    countCrossTrades(RHTOct30Times, IBMOct30Times, crossTradesOct30, i)
    countCrossTrades(RHTOct26Times, IBMOct26Times, crossTradesOct26, i)

try:
    crossTradeDf29 = pd.DataFrame(crossTradesOct29, index=[0])
    crossTradeDf29.to_csv('/nobackup4/murrell/RohitData/CrossTradesOct29ms.csv', index=False)

    crossTradeDf30 = pd.DataFrame(crossTradesOct30, index=[0])
    crossTradeDf30.to_csv('/nobackup4/murrell/RohitData/CrossTradesOct30ms.csv', index=False)

    crossTradeDf26 = pd.DataFrame(crossTradesOct26, index=[0])
    crossTradeDf26.to_csv('/nobackup4/murrell/RohitData/CrossTradesOct26ms.csv', index=False)

except:
    print("Error in df constructor. Values from data:")
    print("OCTOBER 29")
    print(crossTradesOct29)

    print("OCTOBER 30")
    print(crossTradesOct30)

    print("OCTOBER 26")
    print(crossTradesOct26)


