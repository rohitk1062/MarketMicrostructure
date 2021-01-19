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


# Convert to numpy arrays for speed
RHTOct29Times = groupedOct29RHT.index.to_numpy()
IBMOct29Times = groupedOct29IBM.index.to_numpy()

# Will store cross trade data for Oct 29
crossTradesOct29 = {}


def countCrossTrades(times1, times2, data, delta):
    totalCross = 0

    for time in times1:
        timeOffset = time + np.timedelta64(delta, 'us')
        numCross = (times2 == timeOffset).sum()
        totalCross += numCross

    data[delta] = totalCross


for i in range(-100, 101):
    countCrossTrades(RHTOct29Times, IBMOct29Times, crossTradesOct29, i)

try:
    crossTradeDf29 = pd.DataFrame(crossTradesOct29, index=[0])
    crossTradeDf29.to_csv('/Users/rohitk/Documents/SURE Data/CrossTradesOct29ms.csv', index=False)

except:
    print("Error in df constructor. Values from data:")
    print(crossTradesOct29)

