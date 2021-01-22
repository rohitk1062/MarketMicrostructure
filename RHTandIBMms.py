import pandas as pd
import numpy as np

# cluster Path
dfTrades = pd.read_csv("/nobackup4/murrell/RohitData/IBMandRHTTrades.csv")


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

# Grouping by trading days. 3 days before announcement and 3 days after announcement
groupedOct24RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-24']
groupedOct24IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-24']

groupedOct25RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-25']
groupedOct25IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-25']

groupedOct26RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-26']
groupedOct26IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-26']

# ANNOUNCEMENT ON SUNDAY OCT 28

groupedOct29RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-29']
groupedOct29IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-29']

groupedOct30RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-30']
groupedOct30IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-30']

groupedOct31RHT = dfRHTTrades[dfRHTTrades.index.date.astype(str) == '2018-10-31']
groupedOct31IBM = dfIBMTrades[dfIBMTrades.index.date.astype(str) == '2018-10-31']


# Converting times to numpy arrays
RHTOct24Times = groupedOct24RHT.index.to_numpy()
IBMOct24Times = groupedOct24IBM.index.to_numpy()

RHTOct25Times = groupedOct25RHT.index.to_numpy()
IBMOct25Times = groupedOct25IBM.index.to_numpy()

RHTOct26Times = groupedOct26RHT.index.to_numpy()
IBMOct26Times = groupedOct26IBM.index.to_numpy()


RHTOct29Times = groupedOct29RHT.index.to_numpy()
IBMOct29Times = groupedOct29IBM.index.to_numpy()

RHTOct30Times = groupedOct30RHT.index.to_numpy()
IBMOct30Times = groupedOct30IBM.index.to_numpy()

RHTOct31Times = groupedOct31RHT.index.to_numpy()
IBMOct31Times = groupedOct31IBM.index.to_numpy()


crossTradesOct24 = {}
crossTradesOct25 = {}
crossTradesOct26 = {}

crossTradesOct29 = {}
crossTradesOct30 = {}
crossTradesOct31 = {}



def countCrossTrades(times1, times2, data, delta):
    totalCross = 0

    for time in times1:
        timeOffset = time + np.timedelta64(delta, 'us')
        numCross = (times2 == timeOffset).sum()
        totalCross += numCross

    data[delta] = totalCross


#  -10, -9.9ms, ..., 0, 0.1, ... 10ms intervals as microseconds
for i in range(-10000,10001,100):
    countCrossTrades(RHTOct24Times, IBMOct24Times, crossTradesOct24, i)
    countCrossTrades(RHTOct25Times, IBMOct25Times, crossTradesOct25, i)
    countCrossTrades(RHTOct26Times, IBMOct26Times, crossTradesOct26, i)

    countCrossTrades(RHTOct29Times, IBMOct29Times, crossTradesOct29, i)
    countCrossTrades(RHTOct30Times, IBMOct30Times, crossTradesOct30, i)
    countCrossTrades(RHTOct31Times, IBMOct31Times, crossTradesOct31, i)


try:
    crossTradeDf24 = pd.DataFrame(crossTradesOct24, index=[0])
    crossTradeDf24.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct24ms.csv', index=False)

    crossTradeDf25 = pd.DataFrame(crossTradesOct25, index=[0])
    crossTradeDf25.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct25ms.csv', index=False)

    crossTradeDf26 = pd.DataFrame(crossTradesOct26, index=[0])
    crossTradeDf26.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct26ms.csv', index=False)

    crossTradeDf29 = pd.DataFrame(crossTradesOct29, index=[0])
    crossTradeDf29.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct29ms.csv', index=False)

    crossTradeDf30 = pd.DataFrame(crossTradesOct30, index=[0])
    crossTradeDf30.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct30ms.csv', index=False)

    crossTradeDf31 = pd.DataFrame(crossTradesOct31, index=[0])
    crossTradeDf31.to_csv('/nobackup4/murrell/RohitData/IBM_RHTOct31ms.csv', index=False)


except:
    print("Error in df constructor. Values from data:")

    print("OCTOBER 24: ")
    print(crossTradesOct24)

    print("OCTOBER 25: ")
    print(crossTradesOct25)

    print("OCTOBER 26: ")
    print(crossTradesOct26)

    print("OCTOBER 29: ")
    print(crossTradesOct29)

    print("OCTOBER 30: ")
    print(crossTradesOct30)

    print("OCTOBER 31: ")
    print(crossTradesOct31)

