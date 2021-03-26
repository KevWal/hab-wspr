#!/usr/bin/python3

from bs4 import BeautifulSoup  
import configparser
import csv
import datetime
import getopt
import os
import requests
import sqlite3
import sys
import time

from balloon import *
from telemetry import *

#
# Get Spots from wsprnet.org
#
def getspots (nrspots):
    logging.info("getspots() Fetching %d Spots from wsprnet.org old database.", nrspots)
    # KW force 20m only wiki = "http://wsprnet.org/olddb?mode=html&band=all&limit=" + str(nrspots) + "&findcall=&findreporter=&sort=spotnum"
    wiki = "http://wsprnet.org/olddb?mode=html&band=20&limit=" + str(nrspots) + "&findcall=&findreporter=&sort=spotnum"
    try:
        page = requests.get(wiki)
    except requests.exceptions.RequestException as e:
        logging.info("ERROR: %s",e)
        return []

#    logging.info(page.status)
#    logging.info(page.data)

    soup = BeautifulSoup(page.content, 'html.parser')

    data = []
    table = soup.find_all('table')[2]
    # logging.info("TABLE:",table)

    rows = table.findAll('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) # Get rid of empty values

    # Strip empty rows
    newspots = [ele for ele in data if ele] 

    # Strip redundant columns Watt & miles and translate/filter data
    for row in newspots:
        row[0] = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M')
        row[6] = int(row[6].replace('+',''))

        del row[11]
        del row[7]

    # Reverse the sorting order of time to get new spots firsts
    newspots.reverse()

    return newspots

#
# Get Spots from wsprnet.org
#
def gettestspots (nrspots, call):
    logging.info("gettestspots() Fetching %d Spots from wsprnet.org old database.", nrspots)
    wiki = "http://wsprnet.org/olddb?mode=html&band=20&limit=" + str(nrspots) + "&findcall=" + call + "&findreporter=&sort=spotnum"
    try:
        page = requests.get(wiki)
    except requests.exceptions.RequestException as e:
        logging.info("ERROR: %s",e)
        return []

#    logging.info(page.status)
#    logging.info(page.data)

    soup = BeautifulSoup(page.content, 'html.parser')

    data = []
    table = soup.find_all('table')[2]
    # logging.info("TABLE:",table)

    rows = table.findAll('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) # Get rid of empty values

    # Strip empty rows
    newspots = [ele for ele in data if ele]

    # Strip redundant columns Watt & miles and translate/filter data
    for row in newspots:
        row[0] = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M')
        row[6] = int(row[6].replace('+',''))

        del row[11]
        del row[7]

    # Reverse the sorting order of time to get new spots firsts
    newspots.reverse()

    return newspots


# 
# Dump new spots to db. Note stripping of redundant fields
#
# Example: 2018-05-28 05:50,OM1AI,7.040137,-15,0,JN88,+23,DA5UDI,JO30qj,724
#
def dumpnewdb(spotlist):
    con = None
    data = None
    
    try:
        con = sqlite3.connect('wsprdb.db')
        cur = con.cursor()
        cur.execute('create table if not exists newspots(timestamp varchar(20), tx_call varchar(10), freq real, snr integer, drift integer, tx_loc varchar(6), power integer, rx_call varchar(10), rx_loc varchar(6), distance integer)')
        for row in spotlist:
            logging.info(row)
            cur.execute("INSERT INTO newspots VALUES(?,?,?,?,?,?,?,?,?,?)", (row))
            data = cur.fetchall()

        if not data:
            con.commit()
    except sqlite3.Error as e:
        logging.info("Database error: %s", e)
    except Exception as e:
        logging.info("Exception in _query: %s", e)
    finally:
        if con:
            con.close()
    return


#
# Filter out only calls from balloons and telemetry packets
#
def balloonfilter(spots,balloons):
    logging.info("balloonfilter() Filtering Spots to only balloons in ini file and balloon Telemetry packets.")

    filtered = []
    calls = []
    for b in balloons:
        calls.append(b[1])

    for row in spots:
        for c in calls:
            if row[1] == c:

                # Remove selfmade WSPR tranmissions
                if len(row[5]) == 4:
                    filtered.append(row)
                else:
                    row[5] = row[5][0:4]
                    filtered.append(row)

        if re.match('(^0|^Q).[0-9].*', row[1]):
            filtered.append(row)

    for r in filtered:
        logging.info("balloonfilter() Found: %s, %s", r[0], r[1:])

    logging.info("balloonfilter() In: %d Out: %d", len(spots), len(filtered))
    return filtered

#
# De duplicate spots
#
# Example: 2018-05-28 05:50,OM1AI,7.040137,-15,0,JN88,+23,DA5UDI,JO30qj,724
#
def deduplicate(spotlist):
    pre=len(spotlist)
    
    rc = 0
    rc_max = len(spotlist)-1
    if rc_max > 1:
        while rc < rc_max:
            if (spotlist[rc][0] == spotlist[rc+1][0]) and (spotlist[rc][1] == spotlist[rc+1][1]):
#                logging.info("Duplicate entry")
                del spotlist[rc]
                rc_max -= 1
            else:
                rc += 1

    logging.info("deduplicate() In: %d Out: %d", pre, len(spotlist))
    return spotlist


####################################################################################################
# Main
#

# Setup logging
level    = logging.INFO
format   = '%(asctime)s - %(message)s'
handlers = [logging.FileHandler('logging.txt','a'), logging.StreamHandler()]
logging.basicConfig(level = level, format = format, handlers = handlers, datefmt='%y-%m-%d %H:%M:%S')

# Get options from arguments
verbose = False
archive_file = ''
csv_file = ''
conf_file = 'balloon.ini'
dry_run = False
test = False

try:
      options, remainder = getopt.getopt(
            sys.argv[1:],
            'c:f:v:t',
                ['archive=',
                 'csv=',
                 'conf=',
                ])

except getopt.GetoptError as err:
    logging.info('ERROR in options: ', err)
    sys.exit(1)

logging.info("main() Options: %s", str(options))
      
for opt, arg in options:
    if opt in ('--archive'):
        archive_file = arg
    if opt in ('--csv'):
        csv_file = arg
    if opt in ('--conf'):
        conf_file = arg
    if opt in ('--dry_run'):
        dry_run = True
    if opt in ('-t', '--test'):
        test = True
    elif opt in ('-v', '--verbose'):
        verbose = True

# Parse some of balloon config file
config = configparser.ConfigParser()
config.read(conf_file)

push_habhub = config['main']['push_habhub']
habhub_callsign = config['main']['habhub_callsign']

push_aprs = config['main']['push_aprs']

balloons = json.loads(config.get('main','balloons'))
            
logging.info("main() Tracking these balloons:")
for b in balloons:
      logging.info("  %s", str(b))

if dry_run:
    logging.info("main() Dry run. No uploads")
    push_habhub = False
    push_aprs = False
      
spots = []

if test:
      logging.info("main() Test-mode")

      s1 = gettestspots(10000, "PC4L")
      logging.info(len(s1))

      temp_spots = []
      for s in s1:
          if s[0] > datetime.datetime(2020, 11, 3, 0, 0):
              temp_spots.append(s)
      s1 = temp_spots
      logging.info(len(s1))

      s2 = gettestspots(10000, "0*") # Doesnt work, doesnt get 0* spots
      logging.info(len(s2))

      temp_spots = []
      for s in s2:
          if s[0] > datetime.datetime(2020, 11, 3, 0, 0):
              temp_spots.append(s)
      s2 = temp_spots
      logging.info(len(s2))

      spots = s1 + s2

      logging.info(len(spots))

      spots.sort(reverse=False)

      logging.info(len(spots))

      logging.info("Done")
      sys.exit(0)


# Load and process spots from archive-file - default append to csv
if archive_file:
      logging.info("main() Archive-mode")

      # Read archivefile and filter out balloondata
      spots = readgz(balloons, archive_file)
      logging.info(spots[0])
      spots.sort(reverse=False)

      # Do a crude timetrim 
      temp_spots = []
      for s in spots:
          if s[0] > datetime.datetime(2020, 11, 4, 0, 0):
              temp_spots.append(s)
      spots = temp_spots
      
      #dumpcsv(spots)
      #sys.exit(0) # Comment this out to go on and process those spots

      if len(spots) > 1:
            logging.info("Spots: %s", str(len(spots)))
            spots = process_telemetry(spots, balloons,habhub_callsign, push_habhub, push_aprs)
      else:
            logging.info("No spots!")
            
      logging.info("Done")
      sys.exit(0)

# Load and process spots from csv-file
if csv_file:
      logging.info("main() CSV-mode")
      spots = readcsv(csv_file)

      # Do a crude trimetrim 
      # temp_spots = []
      # for s in spots:
      #     if s[0] > datetime.datetime(2019, 12, 18, 11, 0) and s[0] < datetime.datetime(2019, 12, 18, 12, 30):
      #         # logging.info(s)
      #         temp_spots.append(s)
      # spots = temp_spots

      
      if len(spots) > 1:
            logging.info("main() Spots: %s", str(len(spots)))
            spots = process_telemetry(spots, balloons, habhub_callsign, push_habhub, push_aprs)
      else:
            logging.info("No spots!")

      logging.info("Done")
      sys.exit(0)

# Spots to pullfrom wsprnet
nrspots_pull = 8000
spotcache = []

logging.info("main() Preloading spot cache with 10,000 spots...")
spotcache = getspots(10000)
logging.info("main() Got %d spots in cache", len(spotcache))
spotcache = balloonfilter(spotcache, balloons)

spots = spotcache
cache_max = 10000
new_max = 0
only_balloon = False
sleeptime = 90

while 1==1:
    print("\r\n\r\n");
    logging.info("main() Begin polling loop.")
    tnow = datetime.datetime.now() 

    wwwspots = getspots(nrspots_pull)
    wwwspots = balloonfilter(wwwspots ,balloons)
    newspots = [] 

    # Sort cache in case some spots arrived out of order
    spotcache.sort(reverse=False)
    # Use only the last 120 mins of spotcache
    logging.info("main() Timetrim spot cache 120m.")
    spotcache = timetrim(spotcache,120)

    src_cc = 0 

    # Loop through cache and check for new spots
    logging.info("main() Removing spots found in our cache.")
    for row in wwwspots:
        old = 0
        for srow in spotcache:
            # logging.info("testing: ", row, "\nagainst:", srow)
            src_cc += 1
            if row == srow:
                # logging.info("Found %s", row)
                old = 1
                break

        if old == 0:
            logging.info("main() Found new spot: %s, %s", row[0], row[1:])
            
            # Insert into cache at the beginning
            spotcache.insert(0, row)

 #           for w in spotcache:
 #               logging.info("cache2: ", w)

            # and add to newspots at the end
            newspots.append(row)

#     spotcache.sort(reverse=True)
#    logging.info("first:",spotcache[0][0]," last: ",spotcache[-1:][0][0])
#    logging.info("DATA:\n")
#    for row in newspots:
#        logging.info("Newspots: ", row)

#    dumpcsv(newspots)
#    dumpnewdb(newspots)

    logging.info("main() Add %d new balloon spots to %d previous spots.", len(newspots), len(spots))
    spots = spots + newspots
    spots.sort(reverse=False)   
    spots = deduplicate(spots) # needs sorted list
    # Filter out all spots newer than x minutes
    logging.info("main() Timetrim spots to be processed to 60m.")
    spots = timetrim(spots, 60)

    if len(spots) > 1:
        logging.info("main() Passing %d spots to process_telemetry().", len(spots))
        #logging.info(spots);
        spots = process_telemetry(spots, balloons, habhub_callsign, push_habhub, push_aprs)
        logging.info("main() %d spots returned from process_telemetry().", len(spots))

    if new_max < len(newspots):
#  and len(newspots) != nrspots_pull:
        new_max = len(newspots)

    if len(newspots) == nrspots_pull:
        logging.info("main() Hit max spots. Increasing set to fetch")
        nrspots_pull += 100

    logging.info("main() Stats this loop: Spots: %5d Cache: %5d New: %5d (max: %5d) Nrspots: %5d Looptime: %5d (s) Checks: %5d" % 
          (len(spots), len(spotcache), len(newspots), new_max, nrspots_pull, float(str(datetime.datetime.now() - tnow).split(":")[2]), src_cc)) 

    # What does this do?
    spotcache = spotcache[:cache_max]

    sleeping = sleeptime - int(datetime.datetime.now().strftime('%s')) % sleeptime
    logging.info("main() Sleep: %d", sleeping)
    time.sleep(sleeping)







        
