CREATE OR REPLACE FUNCTION
disqualifyHorseInRaceFunction(theHorseID INTEGER, theRacetrackID INTEGER, theRaceDate DATE, theRaceNum INTEGER)
RETURNS INTEGER AS $$

    DECLARE
        numDisqualified        INTEGER;
        theHorseID INTEGER;

    DECLARE disqualifyingCursor CURSOR FOR
            SELECT h.horseID, disqualified.finishPosition
            FROM Horses h, HorseRaceResults disqualified, racetracks rt
            WHERE h.horseID = disqualified.horseID
              AND disqualified.raceNum = rt.raceNum
              AND disqualified.raceDate = rt.raceDate
              AND disqualified.racetrackID = rt.racetrackID
              AND disqualified.finishPosition > 1;

    BEGIN

    --Input Validation
    IF NOT EXISTS (SELECT * FROM horseRaceResults WHERE horseID = theHorseID) THEN
        RETURN -1;
        END IF;

    IF EXISTS (SELECT * FROM horseRaceResults WHERE finishPosition IS NULL) THEN
        RETURN -2;
        END IF;

        numDisqualified := 0;

        OPEN disqualifyingCursor;

        LOOP

            FETCH disqualifyingCursor INTO theHorseID;
            -- Exit if there are no more records for disqualifyingCursor,
            -- or when we already have performed max disqualifying races.
            EXIT WHEN NOT FOUND OR finishPosition IS null;

            SELECT finishPosition AS disqualifiedPositions
            FROM disqualifyHorseInRaceFunction, horseRaceResults hrr
            WHERE hrr.horseID = theHorseID;

            UPDATE HorseRaceResults
            SET finishPosition = NULL
            WHERE horseID = theHorseID;

            UPDATE HorseRaceResults
            SET finishPosition = finishPosition - 1
            WHERE disqualifiedPosition = NULL
            AND finishPosition > disqualifiedPosition;

            numDisqualified := numDisqualified + 1

        END LOOP;
        CLOSE disqualifyingCursor;

    RETURN numDisqualified;

    END

$$ LANGUAGE plpgsql;