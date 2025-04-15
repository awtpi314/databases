import os
import sys
import django
import mysql.connector
import time
from datetime import datetime
from django.db.models import Model

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mlbstats.settings")
django.setup()

from mlbdata.models import (
    Player,
    Position,
    PlayerSeason,
    BattingStats,
    FieldingStats,
    PitchingStats,
    CatchingStats,
)


def connect_to_original_db():
    return mysql.connector.connect(
        host="100.77.203.81", user="awtpi", password="password", database="mlb_original"
    )


def add_positions(players: dict[str, Player]):
    # First, ensure all positions exist in the new database
    positions = {
        "P": "Pitcher",
        "C": "Catcher",
        "1B": "First Base",
        "2B": "Second Base",
        "3B": "Third Base",
        "SS": "Shortstop",
        "OF": "Outfield",
        "LF": "Left Field",  # Not Used
        "CF": "Center Field",  # Not Used
        "RF": "Right Field",  # Not Used
        "DH": "Designated Hitter",  # Not Used
    }

    # Create all position objects if they don't exist
    for code in positions:
        Position.objects.get_or_create(position_code=code)

    db_positions = Position.objects.all()

    # Connect to original database
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Query to get positions for each player
    cursor.execute(
        """
        SELECT DISTINCT playerID, POS 
        FROM fielding 
    """
    )

    db_players = {p.player_id: p for p in Player.objects.all()}
    for player_id, player_value in players.items():
        print(f"Player ID: {player_id}, Player Value: {player_value}")
        if player_value.player_id in db_players:
            players[player_id] = db_players[player_value.player_id]

    positions_to_create = []

    position_data = cursor.fetchall()

    position_dict = {pos.position_code: pos for pos in db_positions}

    for row in position_data:
        pid = row["playerID"]
        pos_code = row["POS"]

        if (
            pos_code not in positions
            or pid not in players
            or pos_code not in position_dict
        ):
            continue

        positions_to_create.append((players[pid], position_dict[pos_code]))

    print(f"Adding {len(positions_to_create)} positions to {len(players)} players")

    # Create position assignments using bulk_create with batches of 1000

    # Get the through model for the many-to-many relationship
    player_position_model = Player.positions.through

    # Batch process the positions
    batch_size = 1000
    position_batch = []

    for player, position in positions_to_create:
        position_batch.append(player_position_model(player=player, position=position))

        if len(position_batch) >= batch_size:
            player_position_model.objects.bulk_create(
                position_batch, ignore_conflicts=True
            )
            print(f"Added batch of {len(position_batch)} player-position relationships")
            position_batch = []

    # Create any remaining positions
    if position_batch:
        player_position_model.objects.bulk_create(position_batch, ignore_conflicts=True)
        print(
            f"Added final batch of {len(position_batch)} player-position relationships"
        )

    cursor.close()
    conn.close()


def retrieve_players():
    # Connect to original database
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)  # Returns results as dictionaries

    players = {}
    # Query to get all players
    cursor.callproc("getPlayers")

    players_to_create = []

    # Iterate through results and create Player instances
    for row in next(cursor.stored_results()).fetchall():
        # Convert string dates to Python date objects, handling NULL values
        pid = row["playerId"]
        first_name = row["nameFirst"]
        last_name = row["nameLast"]

        # If the playerId or name is non-existant, skip.
        if (
            pid is None
            or not pid
            or first_name is None
            or not first_name
            or last_name is None
            or not last_name
        ):
            continue

        if row["birthYear"] is None:
            continue
        # Some players only have a birth year.
        elif row["birthMonth"] is None or row["birthDay"] is None:
            birth_day = datetime(year=row["birthYear"], month=1, day=1)
        else:
            birth_day = datetime(
                year=row["birthYear"], month=row["birthMonth"], day=row["birthDay"]
            )

        if row["deathYear"] is None:
            death_day = None
        elif row["deathMonth"] is None or row["deathDay"] is None:
            death_day = datetime(year=row["deathYear"], month=1, day=1)
        else:
            death_day = datetime(
                year=row["deathYear"], month=row["deathMonth"], day=row["deathDay"]
            )
        first_game = (
            datetime.strptime(row["debut"], "%Y-%m-%d").date() if row["debut"] else None
        )
        last_game = (
            datetime.strptime(row["finalGame"], "%Y-%m-%d").date()
            if row["finalGame"]
            else None
        )

        players_to_create.append(
            (
                pid,
                Player(
                    name=first_name + " " + last_name,
                    given_name=row["nameGiven"],
                    birthdate=birth_day,
                    deathdate=death_day,
                    batting_hand=row["bats"],
                    throwing_hand=row["throws"],
                    birth_city=row["birthCity"],
                    birth_state=row["birthState"],
                    birth_country=row["birthCountry"],
                    first_game=first_game,
                    last_game=last_game,
                ),
            )
        )
        if len(players_to_create) >= 1000:
            Player.objects.bulk_create([t[1] for t in players_to_create])
            new_objects = sorted(list(Player.objects.all()), key=lambda x: x.player_id, reverse=True)[-1000:]
            print(f"Added batch of {len(new_objects)} players")
            for db_player, (player_id, _) in zip(new_objects, players_to_create):
                players[player_id] = db_player
            players_to_create = []

    # Create any remaining players
    if players_to_create:
        Player.objects.bulk_create([t[1] for t in players_to_create])

    cursor.close()
    conn.close()

    return players


def add_seasons(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Combined query to get games played and salary data in one go
    cursor.callproc("addSeasons")

    for row in cursor.stored_results():
        pid = row["playerID"]
        yid = row["yearID"]
        tid = row["teamID"]

        p = players.get(pid)
        if p is None:
            continue

        ps = p.seasons.filter(year=yid).first()
        if ps is None:
            ps = PlayerSeason.objects.create(
                player=p,
                year=yid,
                games_played=row["gamesPlayed"],
                salary=row["totalSalary"] if row["totalSalary"] is not None else 0,
            )
        else:
            ps.games_played += row["gamesPlayed"]
            if row["totalSalary"]:  # Only update salary if it exists
                ps.salary += row["totalSalary"]
            ps.save()
        print(f"Created player-season: {pid}, {yid}")

        # TODOAdd team_seasons

    cursor.close()
    conn.close()


def add_batting_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)  # Returns results as dictionaries

    # Query to get discover info each player season
    cursor.execute(
        """
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
        group by playerID, yearID
    """
    )

    for row in cursor.fetchall():
        pid = row["playerID"]
        yid = row["yearID"]
        p = players.get(pid)
        if p is None:
            continue
        ps = p.seasons.filter(year=yid).first()
        if ps is not None:
            BattingStats.objects.create(
                player_season=ps,
                at_bats=row["atBats"],
                hits=row["hits"],
                doubles=row["doubles"],
                triples=row["triples"],
                home_runs=row["homeRuns"],
                runs_batted_in=row["runsBattedIn"],
                strikeouts=row["strikeouts"],
                walks=row["walks"],
                hits_by_pitch=row["hitByPitch"],
                intentional_walks=row["intentionalWalks"],
                steals=row["steals"],
                steals_attempted=row["stealsAttempted"],
            )
            print(f"Added batting stats to {p.name}'s {yid} season")

    cursor.close()
    conn.close()


def add_fielding_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Combined query for both fielding and catching stats
    cursor.execute(
        """
        SELECT playerID, yearID,
               SUM(E) as errors,
               SUM(PO) as putOuts,
               SUM(CASE WHEN POS = 'C' THEN PB ELSE 0 END) as passedBalls,
               SUM(CASE WHEN POS = 'C' THEN WP ELSE 0 END) as wildPitches,
               SUM(CASE WHEN POS = 'C' THEN SB ELSE 0 END) as stealsAllowed,
               SUM(CASE WHEN POS = 'C' THEN CS ELSE 0 END) as stealsCaught,
               MAX(CASE WHEN POS = 'C' THEN 1 ELSE 0 END) as isCatcher
        FROM fielding
        GROUP BY playerID, yearID
    """
    )

    for row in cursor.fetchall():
        pid = row["playerID"]
        yid = row["yearID"]
        p = players.get(pid)
        if p is None:
            continue

        ps = p.seasons.filter(year=yid).first()
        if ps is not None:
            # Create fielding stats for all players
            FieldingStats.objects.create(
                player_season=ps, errors=row["errors"], put_outs=row["putOuts"]
            )
            print(f"Added fielding stats to {p.name}'s {yid} season")

            # Create catching stats only if they played as catcher
            if row["isCatcher"]:
                CatchingStats.objects.create(
                    player_season=ps,
                    passed_balls=row["passedBalls"],
                    wild_pitches=row["wildPitches"],
                    steals_allowed=row["stealsAllowed"],
                    steals_caught=row["stealsCaught"],
                )
                print(f"Added catching stats to {p.name}'s {yid} season")

    cursor.close()
    conn.close()


def add_pitching_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Query to get discover info each player season
    cursor.execute(
        """
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
        group by playerID, yearID
    """
    )

    for row in cursor.fetchall():
        pid = row["playerID"]
        yid = row["yearID"]
        p = players.get(pid)
        if p is None:
            continue
        ps = p.seasons.filter(year=yid).first()
        if ps is not None:
            PitchingStats.objects.create(
                player_season=ps,
                outs_pitched=row["outsPitched"],
                earned_runs_allowed=row["earnedRunsAllowed"],
                home_runs_allowed=row["homeRunsAllowed"],
                strikeouts=row["strikeouts"],
                walks=row["walks"],
                wins=row["wins"],
                losses=row["losses"],
                wild_pitches=row["wildPitches"],
                batters_faced=row["battersFaced"],
                hit_batters=row["hitBatters"],
                saves=row["saves"],
            )
            print(f"Added pitching stats to {p.name}'s {yid} season")

    cursor.close()
    conn.close()


# Main function
if __name__ == "__main__":
    start_time = time.time()

    players = retrieve_players()
    # add_positions(players)
    # add_seasons(players)
    # add_batting_stats(players)
    # add_fielding_stats(players)
    # add_pitching_stats(players)
    # persist all the objects

    end_time = time.time()
    duration = end_time - start_time
    print(f"Conversion took {duration:.2f} seconds")
