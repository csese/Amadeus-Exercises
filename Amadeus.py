__author__ = 'Charles Sese'

import pandas as pd
from GeoBases import GeoBase
import matplotlib.pyplot  as plt
### Exercise 1 ####
def nbrLine(filename):
    nLine = 0
    with open(filename, "r") as f:
        for line in f:
            nLine += 1
    return (nLine)

print 'The number of lines for bookings.csv is' , nbrLine('./data/bookings.csv')
print 'The number of lines for searches.csv is' , nbrLine('./data/searches.csv')

### Exercise 2 ####

def topAirports(filename, n = 10):
    paxArr = pd.DataFrame(columns=['arr_port','pax'])
    reader = pd.read_table(filename, sep='^', chunksize = 100000, usecols=['arr_port','pax'])
    for chunk in reader:
        chunk.columns = chunk.rename(columns=lambda x: x.strip()).columns.values
        paxArr = pd.concat([paxArr, chunk[['arr_port','pax']].groupby('arr_port').sum().reset_index()])

    topAirport = paxArr[['arr_port','pax']].groupby('arr_port').sum().sort('pax', ascending = 0).reset_index()[0:(n-1)]
    for k, airport in enumerate(topAirport['arr_port']):
        topAirport.loc[k,'airport'] = geo_o.get(airport.strip(), 'name', default="Undefined")
    print "\nTop 10 arrival airports:\n%s" % topAirport
    return(topAirport)

geo_o = GeoBase(data='ori_por', verbose=False)
topAirport = topAirports('./data/bookings.csv')

### Exercise 3 ####
def destinationAirport(filename, destination):
    reader = pd.read_table(filename, sep='^', chunksize = 10000, usecols=['Date','Destination'])
    totSearches = pd.DataFrame(columns = ['Month',destination])
    for chunk in reader:
        # chunk = reader.get_chunk(5000000)
        # chunk = chunk[['Date','Destination']]
        # chunk = chunk.query('["'+destination+'"] in Destination')
        chunk = chunk[chunk['Destination'] ==  destination]
        # chunk['count'] = 1
        chunk['Date']  = pd.to_datetime(chunk['Date'])
        chunk['Month'] = chunk['Date'].dt.month
        # chunk = chunk.groupby(['Month'], as_index = False).agg({'count':sum})
        chunk = chunk.groupby(['Month'], as_index = False).count()

        # chunk = chunk.groupby([pd.Grouper(freq='1M', key='Date')]).count()
        del chunk['Destination']
        chunk.columns = ['Month',destination]
        totSearches = pd.concat([totSearches, chunk])
    totSearches = totSearches.groupby('Month').sum()

    return totSearches

def log(msg):
    """To log message with datetime."""
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S %f") + ': ' + msg

from datetime import datetime


filename = './data/searches.csv'
log('Retrieving Data for Malaga')
totMalaga = destinationAirport(filename, 'AGP')
log('Retrieving Data for Madrid')
totMadrid = destinationAirport(filename, 'MAD')
log('Retrieving Data for Barcelona')
totBarcelona = destinationAirport(filename, 'BCN')

plt.plot(totMadrid)
plt.plot(totMalaga)
plt.plot(totBarcelona)

#set plot attributes
plt.legend(('Madrid','Malaga','Barcelona'))
plt.xticks(range(12),['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November','December'], rotation=45)
plt.xlabel('Month')
plt.ylabel('Number of Searches')
plt.grid()

plt.show()

#### Exercise 4 ########

booking = pd.read_table('./data/bookings.csv', sep='^', chunksize = 10000)
searches = pd.read_table('./data/searches.csv', sep='^', chunksize = 10000)

bookingList = list()
for chunkBooking in booking:
    zipped = zip(map(str.strip, chunkBooking['dep_port']) , map(str.strip, chunkBooking['arr_port']))
    bookingList.extend(zipped)
    seen = set()
    bookingList = [item for item in bookingList if item[1] not in seen and not seen.add(item[1])]

searchesIndex = 0

for chunkSearches in searches:
    booked = list()
    for k in range(0,len(chunkSearches)):
        searchesIndex += 1
        bookedSearch = [False]*len(chunkSearches)
        fromSearch = chunkSearches.ix[k, 'Origin']
        toSearch = chunkSearches.ix[k, 'Destination']
        if (fromSearch, toSearch) in bookingList:
            booked.append(1)
        else:
            booked.append(0)
    chunkToWrite = pd.concat([chunkSearches, pd.DataFrame(booked, columns= ['booked'])], axis=1)
    chunkToWrite.to_csv(path='searches_matched.csv', sep=',', encoding='utf-8',mode='a')
    print(searchesIndex)
