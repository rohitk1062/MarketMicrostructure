import pandas as pd
from numba import jit
import numpy as np

# Give info about Target and Acquirer ticker
Target = "AET"
Acquirer = "CVS"
cols1 = ['Datetime','SIZE','PRICE','BID','BIDSIZ','ASK','ASKSIZ','Direction']
cols2 = ['Datetime','Symbol','SIZE','PRICE','BID','BIDSIZ','ASK','ASKSIZ','Direction']

# -----------------------------------------------------------------------------------------------------------------
# Read data
dfTrades = pd.read_csv("/Volumes/ROHIT/SURE Data/CVSandAetnaTrades.csv")
dfQuotes = pd.read_csv("/Volumes/ROHIT/SURE Data/CVSandAetnaQuotes.csv")

# -----------------------------------------------------------------------------------------------------------------
# Create datetime col Trades
dfTrades = dfTrades.drop(columns=['SYM_SUFFIX'])
dfTrades['Datetime'] = pd.to_datetime(dfTrades["DATE"].astype(str) + " " + dfTrades["TIME_M"].astype(str))
dfTrades = dfTrades.drop(columns=['DATE', 'TIME_M'])
dfTrades = dfTrades.sort_values(by=['Datetime'])

# Create datetime col Quotes
dfQuotes = dfQuotes.drop(columns=['SYM_SUFFIX'])
dfQuotes['Datetime'] = pd.to_datetime(dfQuotes["DATE"].astype(str) + " " + dfQuotes["TIME_M"].astype(str))
dfQuotes = dfQuotes.drop(columns=['DATE', 'TIME_M'])
dfQuotes = dfQuotes.sort_values(by=['Datetime'])

# -----------------------------------------------------------------------------------------------------------------
# Split quotes and trades by ticker
TargetTrades = dfTrades[dfTrades["SYM_ROOT"] == Target]
AcquirerTrades = dfTrades[dfTrades["SYM_ROOT"] == Acquirer]

TargetQuotes = dfQuotes[dfQuotes["SYM_ROOT"] == Target]
AcquirerQuotes = dfQuotes[dfQuotes["SYM_ROOT"] == Acquirer]

# -----------------------------------------------------------------------------------------------------------------
# For each Target trade merge with nearest quote within 5ms
mergedTarget = pd.merge_asof(TargetTrades, TargetQuotes, on='Datetime', tolerance=pd.Timedelta("5ms"), direction='nearest')
mergedTarget = mergedTarget.drop(columns=['SYM_ROOT_y','SYM_ROOT_x'])
mergedTarget["Direction"] = 0
mergedTarget = mergedTarget.dropna()
mergedTarget = mergedTarget.reindex(columns=cols1)
mergedTarget['Datetime'] = mergedTarget['Datetime'].values.astype(np.int64) // 10**3

# -----------------------------------------------------------------------------------------------------------------
# For each Acquirer trade merge with nearest quote within 5ms
mergedAcquirer = pd.merge_asof(AcquirerTrades, AcquirerQuotes, on='Datetime', tolerance=pd.Timedelta("5ms"), direction='nearest')
mergedAcquirer = mergedAcquirer.drop(columns=['SYM_ROOT_y','SYM_ROOT_x'])
mergedAcquirer["Direction"] = 0
mergedAcquirer = mergedAcquirer.dropna()
mergedAcquirer = mergedAcquirer.reindex(columns=cols1)
mergedAcquirer['Datetime'] = mergedAcquirer['Datetime'].values.astype(np.int64) // 10**3

# -----------------------------------------------------------------------------------------------------------------
# Make sure types are okay for numba
mergedTarget = mergedTarget.apply(pd.to_numeric)
mergedTargetArray = mergedTarget.to_numpy(dtype='float32')

mergedAcquirer = mergedAcquirer.apply(pd.to_numeric)
mergedAcquirerArray = mergedAcquirer.to_numpy(dtype='float32')

# -----------------------------------------------------------------------------------------------------------------
# Columns: Datetime(0) SIZE(1)	PRICE(2)	BID(3)	BIDSIZ(4)	ASK(5)	ASKSIZ(6)	Direction(7)
# Lee ready algorithm to sign trades

@jit(nopython=True)
def leeReady(arr):
    prevPrice = 0

    for row in arr:
        midpoint = (row[3] + row[5]) / 2

        if row[2] > midpoint:
            row[7] = 1
        elif row[2] < midpoint:
            row[7] = -1

        else:
            if prevPrice != 0:
                if row[2] > prevPrice:
                    row[7] = 1
                elif row[2] < prevPrice:
                    row[7] = -1

        prevPrice = row[2]


leeReady(mergedTargetArray)
leeReady(mergedAcquirerArray)

# -----------------------------------------------------------------------------------------------------------------
# prep final target df
finalTarget = pd.DataFrame(mergedTargetArray, columns=cols1)
finalTarget = finalTarget[finalTarget['Direction'] != 0]
finalTarget['Symbol'] = Target
finalTarget.reindex(columns=cols2)
finalTarget['Datetime'] = pd.to_datetime(finalTarget['Datetime'], unit='us')
finalTarget.set_index('Datetime')

# -----------------------------------------------------------------------------------------------------------------
# prep final acquirer df
finalAcquirer = pd.DataFrame(mergedAcquirerArray, columns=cols1)
finalAcquirer = finalAcquirer[finalAcquirer['Direction'] != 0]
finalAcquirer['Symbol'] = Acquirer
finalAcquirer.reindex(columns=cols2)
finalAcquirer['Datetime'] = pd.to_datetime(finalAcquirer['Datetime'], unit='us')
finalAcquirer.set_index('Datetime')

# -----------------------------------------------------------------------------------------------------------------
# Write dfs
try:
    finalTarget.to_csv("/Volumes/ROHIT/SURE Data/" + Target + "Signed.csv")
    finalAcquirer.to_csv("/Volumes/ROHIT/SURE Data/" + Acquirer + "Signed.csv")
except:
    print("Error on writing to csv")
