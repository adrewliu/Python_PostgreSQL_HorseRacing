import psycopg2
import sys
import datetime


def usage():
    print("Usage:  python3 runHorseRacingApplication.py userid pwd", file=sys.stderr)
    sys.exit(-1)


'''
winCountForHorse (myConn, theHorseID):
The winCountForHorse Python function returns the number of times that a horse particular ID
finished first in a race.  The arguments for winCountForHorse  are the database connection and
the ID of the horse.

It is not an error if there is no horse with that ID.  If there are no races in which the horse
finished first, then winCountForHorse  should return 0.
'''
def winCountForHorse (myConn, theHorseID):
    try:
        myCursor = myConn.cursor()
        winCountForHorse = "SELECT horseID, count(*) FROM HorseRaceResults WHERE finishPosition = 1 GROUP BY horseID HAVING count(*) > 0"
        myCursor.execute(winCountForHorse)
    except:
        print("selection from HorseRaceResults had error", file=sys.stderr)
        myCursor.close()
        myConn.close()
        sys.exit(-1)
    rows = myCursor.fetchall()
    for row in rows:
        if row[0] == theHorseID:
            return row[1]
    myCursor.close()
    return '0'


'''
updateRacetrackAddress (myConn, oldAddress, newAddress ):
address is an attribute of the racetracks table.  Sometimes the address of a racetrack may change.

Besides the database connection, the updateRacetrackAddress Python function has two arguments, a
string argument oldAddress and a string argument newAddress.  For every racetrack in the racetracks
table (if any) whose address equals oldAddress, updateRacetrackAddress  should change that
racetrack’s address to be newAddress.

There might be no racetrack whose address equals oldAddress; that’s not an error.  There also might
be one or more racetracks whose address equals oldAddress (since multiple racetracks might have
the same address).  updateRacetrackAddress should return the number of racetracks whose address
was updated.
'''
def updateRacetrackAddress (myConn, oldAddress, newAddress):
    try:
        myCursor = myConn.cursor()
        print("updating...")
        myCursor.execute("UPDATE Racetracks SET address = %s WHERE address = %s", (newAddress, oldAddress))
        numOfUpdates = myCursor.rowcount
        print("# of rows updated: ", numOfUpdates)
        return numOfUpdates
    except:
        print("updating address, had error", file=sys.stderr)
        myCursor.close()
        myConn.close()
        sys.exit(-1)
    myCursor.close()
    return numOfUpdates


'''
disqualifyHorseInRace (myConn, theHorseID, theRacetrackID, theRaceDate, theRaceNum):
Besides the database connection, this Python function has four other parameters:    
    theHorseID which is an integer,
    theRacetrackID which is an integer,
    theRaceDate which is a date,
    theRaceNum, which is an integer
disqualifyHorseInRace invokes a Stored Function, disqualifyHorseInRaceFunction.
The Stored Function disqualifyHorseInRaceFunction has all the same parameters as
disqualifyHorseInRace (except for the database connection, which is not a parameter for the
Stored Function), and it returns an integer.
'''
def disqualifyHorseInRace (myConn, theHorseID, theRacetrackID, theRaceDate, theRaceNum):
    try:
        myCursor = myConn.cursor()
        sql = "SELECT disqualifyHorseInRaceFunction(%s, %s, %s, %s)"
        myCursor.execute(sql(theHorseID, theRacetrackID, theRaceDate, theRaceNum))
        #print("Data base connected!)")
    except:
        print("Call of disqualifyHorseInRaceFunction on", theHorseID, theRacetrackID, theRaceDate, theRaceNum, "had error", file=sys.stderr)
        myCursor.close()
        myConn.close()
        sys.exit(-1)
    
    row = myCursor.fetchone()
    myCursor.close()
    return(row[0])


def main():
    if len(sys.argv)!=3:
       usage()

    hostname = "cse182-db.lt.ucsc.edu"
    userID = sys.argv[1]
    pwd = sys.argv[2]

    # connection to the database
    try:
        myConn = psycopg2.connect(host=hostname, user=userID, password=pwd)
        print("Success!")

        # Function testing

        winCountForHorse(myConn, 526)
        # Horse 526 won 0 races
        winCountForHorse(myConn, 555)
        # Horse 555 won 3 races
        updateRacetrackAddress(myConn, "Kellogg Rd 6301, Cincinnati, OH 45230", "6301 Kellogg Road, Cincinnati, OH 45230")
        # Number of racetracks with whose address was changed from Kellogg Rd 6301, Cincinnati, OH 45230 to 6301 Kellogg Road, Cincinnati, OH 45230 is 3
        updateRacetrackAddress(myConn, "Elmont, NY 11003", "Belmont Park, NY 11003")
        # Elmont, NY 11003, Cincinnati, OH 45230 to Belmont Park, NY 11003 is 0

    except:
        print("Connection to database failed", file=sys.stderr)
        sys.exit(-1)

    myConn.rollback()
    myConn.autocommit = True
    myConn.close()
    sys.exit(0)
#end

#------------------------------------------------------------------------------
if __name__=='__main__':
    main()
    myConn.close()

# end

