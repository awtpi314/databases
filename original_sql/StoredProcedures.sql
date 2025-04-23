DELIMITER $$

CREATE PROCEDURE addBattingStats()
BEGIN
        select playerID, yearID,
        sum(AB) as atBats, 
        sum(H) as hits, 
        sum(2B) as doubles,
        sum(3B) as triples, 
        sum(HR) as homeRuns,
        sum(RBI) as runsBattedIn, 
        sum(SO) as strikeouts,
        sum(BB) as walks, 
        sum(HBP) as hitByPitch, 
        sum(IBB) as intentionalWalks, 
        sum(SB) as steals, 
        sum(CS) as stealsAttempted
        from batting
        group by playerID, yearID;
END $$

CREATE PROCEDURE addFieldingStats()
BEGIN
        SELECT playerID, yearID,
               SUM(E) as errors,
               SUM(PO) as putOuts,
               SUM(CASE WHEN POS = 'C' THEN PB ELSE 0 END) as passedBalls,
               SUM(CASE WHEN POS = 'C' THEN WP ELSE 0 END) as wildPitches,
               SUM(CASE WHEN POS = 'C' THEN SB ELSE 0 END) as stealsAllowed,
               SUM(CASE WHEN POS = 'C' THEN CS ELSE 0 END) as stealsCaught,
               MAX(CASE WHEN POS = 'C' THEN 1 ELSE 0 END) as isCatcher
        FROM fielding
        GROUP BY playerID, yearID;
END $$

CREATE PROCEDURE addPitchingStats()
BEGIN
	select playerID, yearID,
    	sum(IPOuts) as outsPitched,
        sum(ER) as earnedRunsAllowed, 
        sum(HR) as homeRunsAllowed, 
        sum(SO) as strikeouts, 
        sum(BB) as walks, 
        sum(W) as wins, 
        sum(L) as losses, 
        sum(WP) as wildPitches, 
        sum(BFP) as battersFaced, 
        sum(HBP) as hitBatters, 
        sum(SV) as saves
	from pitching 
    group by playerID, yearID;
END $$

CREATE PROCEDURE getPlayers()
BEGIN
SELECT playerId, nameFirst, nameLast, nameGiven, birthDay, birthMonth, birthYear, deathDay, deathMonth, deathYear, bats, throws, birthCity, birthState, birthCountry, debut, finalGame
from people;
END $$

CREATE PROCEDURE getPlayerSeasons()
BEGIN
	SELECT b.playerID, b.yearID, b.teamID, b.lgID,
        SUM(b.G) as gamesPlayed,
    	SUM(s.salary) as totalSalary
	FROM batting b
	LEFT JOIN salaries s ON b.playerID = s.playerID 
		AND b.yearID = s.yearID
    GROUP BY b.playerID, b.yearID, b.teamID, b.lgID;
END $$

CREATE PROCEDURE getPositions(IN player_ids LONGTEXT)
BEGIN
	SET @sql = CONCAT('
		SELECT DISTINCT playerID, POS
		FROM fielding
		WHERE playerID IN (', player_ids, ')');
	PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE getTeams()
BEGIN
with founded_year as (
                select 
                    teamID,
                    min(yearID) as yearFounded
                from 
                    teams
                group by
                    teamID
            ),
            most_recent_info as (
                select 
                    t.teamID,
                    t.name,
                    t.lgID,
                    t.yearID,
                    row_number() over (partition by t.teamID order by t.yearID desc) as rn
                from 
                    teams t
            )
            select 
                m.teamID,
                m.name as most_recent_name,
                m.lgID as most_recent_league,
                f.yearFounded as year_founded,
                m.yearID as most_recent_year
            from 
                most_recent_info m
            join 
                founded_year f on m.teamID = f.teamID
            where 
                m.rn = 1
            order by 
                m.teamID;
END $$

CREATE PROCEDURE getTeamSeasons()
BEGIN
	select
       teamID,
       yearID, 
       sum(G) as "games",
       sum(W) as "wins",
       sum(L) as "losses",
       sum(teamRank) as "rank",
       sum(attendance) as "attendance"
    from teams
    group by teamID, yearID;
END $$