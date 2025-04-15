from math import pi
import os
import sys
import django
from django import db
import mysql.connector
import time
from datetime import datetime

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
    Team,
    TeamSeason,
)


def connect_to_original_db():
    return mysql.connector.connect(
        host="awtpi-server", user="awtpi", password="password", database="mlb_original"
    )


def add_positions(players):
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

    # Connect to original database
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Query to get positions for each player
    cursor.callproc("getPositions", list(players.keys()))

    needed_positions: set[tuple[Player, Position]] = set()
    db_positions = {p.position_code: p for p in Position.objects.all()}
    player_positions_type = Player.positions.through
    # Add positions to each player
    for row in next(cursor.stored_results()):
        player = players[row["playerID"]]
        position = db_positions.get(row["POS"])
        needed_positions.add((player, position))

        if len(needed_positions) >= 1000:
            player_positions_type.objects.bulk_create(
                player_positions_type(
                    player_id=player.player_id, position_id=position.position_code
                )
                for player, position in needed_positions
            )
            print(f"Bulk created {len(needed_positions)} player positions")
            needed_positions.clear()

    if needed_positions:
        player_positions_type.objects.bulk_create(
            player_positions_type(
                player_id=player.player_id, position_id=position.position_code
            )
            for player, position in needed_positions
        )
        print(f"Bulk created final {len(needed_positions)} player positions")

    cursor.close()
    conn.close()


def retrieve_players():
    # Connect to original database
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)  # Returns results as dictionaries

    players = {}
    # Query to get all players
    cursor.callproc("getPlayers")

    player_objects = []

    # Iterate through results and create Player instances
    for row in next(cursor.stored_results()):
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

        player = Player(
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
        )
        player_objects.append(player)
        players[pid] = player

        if len(player_objects) >= 1000:
            Player.objects.bulk_create(player_objects)
            print(f"Bulk created {len(player_objects)} players")
            player_objects = []

    if player_objects:
        Player.objects.bulk_create(player_objects)
        print(f"Bulk created final {len(player_objects)} players")

    cursor.close()
    conn.close()

    db_players = {(p.name, p.birthdate): p for p in Player.objects.all()}
    for pid, player in list(players.items()):
        db_player = db_players.get((player.name, player.birthdate.date()))
        if db_player is None:
            print(f"Warning: Player {player.name} not found in database")
            continue
        players[pid] = db_player
        print(
            f"Retrieved DB ID for player: {db_player.name} (ID: {db_player.player_id})"
        )

    add_positions(players)

    return players


def retrieve_teams():
    # Connect to original database
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)  # Returns results as dictionaries

    teams = {}
    # Query to get all teams
    cursor.callproc("getTeams")

    teams_objects = []

    for row in next(cursor.stored_results()):
        team_id = row["teamID"]
        name = row["most_recent_name"]
        league = row["most_recent_league"]
        year_founded = row["year_founded"]
        most_recent_year = row["most_recent_year"]

        # Create a new Team instance
        team = Team(
            name=name,
            league=league,
            year_founded=year_founded,
            most_recent_year=most_recent_year,
        )
        teams_objects.append(team)
        teams[team_id] = team

        if len(teams_objects) >= 1000:
            Team.objects.bulk_create(teams_objects)
            print(f"Bulk created {len(teams_objects)} teams")
            teams_objects = []

    if teams_objects:
        Team.objects.bulk_create(teams_objects)
        print(f"Bulk created final {len(teams_objects)} teams")

    cursor.close()
    conn.close()

    db_teams = {t.name: t for t in Team.objects.all()}
    for team_id, team in list(teams.items()):
        team_name = teams[team_id].name
        db_team = db_teams.get(team_name)
        if db_team is None:
            print(f"Warning: Team {team.name} not found in database")
            continue
        teams[team_id] = db_team
        print(
            f"Retrieved DB ID for team: {db_team.name} (ID: {db_team.id})"
        )

    return teams


def add_seasons(players, teams: dict):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Combined query to get games played and salary data in one go
    cursor.callproc("getPlayerSeasons")

    print("Creating player seasons...")
    seasons: dict[tuple, PlayerSeason] = {}
    for row in next(cursor.stored_results()):
        pid = row["playerID"]
        yid = row["yearID"]
        tid = row["teamID"]

        if pid not in players:
            continue
        p = players[pid]

        if (yid, pid) not in seasons:
            seasons[(yid, pid)] = PlayerSeason(
                player=p,
                year=yid,
                games_played=row["gamesPlayed"],
                salary=row["totalSalary"] if row["totalSalary"] is not None else 0,
            )
        else:
            ps = seasons[(yid, pid)]
            ps.games_played += row["gamesPlayed"]
            if row["totalSalary"]:
                ps.salary += row["totalSalary"]

    print(f"Created {len(seasons)} player seasons")

    player_season_objects = list(seasons.values())
    for i in range(0, len(player_season_objects), 1000):
        batch = player_season_objects[i : i + 1000]
        PlayerSeason.objects.bulk_create(batch)
        print(f"Bulk created {len(batch)} player seasons")

    cursor.callproc("getTeamSeasons")

    team_seasons: list[TeamSeason] = []
    for row in next(cursor.stored_results()):
        teamID = row["teamID"]
        yearID = row["yearID"]
        if teamID not in teams:
            continue
        t = teams[teamID]

        if (yearID, teamID) not in team_seasons:
            team_seasons.append(
                TeamSeason(
                    team=t,
                    year=yearID,
                    wins=row["wins"],
                    losses=row["losses"],
                    games_played=row["games"],
                    rank=row["rank"],
                    total_attendance=row["attendance"],
                )
            )
          
        if len(team_seasons) >= 1000:
            TeamSeason.objects.bulk_create(team_seasons)
            print(f"Bulk created {len(team_seasons)} team seasons")
            team_seasons = []

    if team_seasons:
        TeamSeason.objects.bulk_create(team_seasons)
        print(f"Bulk created final {len(team_seasons)} team seasons")

    cursor.close()
    conn.close()


def add_batting_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)  # Returns results as dictionaries

    # Query to get discover info each player season
    cursor.callproc("addBattingStats")

    print("Getting player seasons...")
    player_seasons = PlayerSeason.objects.select_related("player").all()
    print("Got player seasons")
    print("Mapping players...")
    player_map = {p.player_id: k for k, p in players.items()}
    print("Mapped players")
    print("Mapping player seasons...")
    player_season_map = {
        (player_map[ps.player.player_id], ps.year): ps for ps in player_seasons
    }
    print("Mapped player seasons")
    needed_stats = []
    for row in next(cursor.stored_results()):
        pid = row["playerID"]
        yid = row["yearID"]
        if pid not in players:
            continue
        p = players[pid]
        if (pid, yid) not in player_season_map:
            print(f"Warning: player season for {pid}, {yid} was not found in the map")
            continue
        ps = player_season_map[(pid, yid)]
        needed_stats.append(
            BattingStats(
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
        )

        if len(needed_stats) >= 1000:
            BattingStats.objects.bulk_create(needed_stats)
            print(f"Bulk created {len(needed_stats)} batting stats")
            needed_stats = []

    if needed_stats:
        BattingStats.objects.bulk_create(needed_stats)
        print(f"Bulk created final {len(needed_stats)} batting stats")

    cursor.close()
    conn.close()


def add_fielding_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Combined query for both fielding and catching stats
    cursor.callproc("addFieldingStats")

    print("Getting player seasons...")
    player_seasons = PlayerSeason.objects.select_related("player").all()
    print("Got player seasons")
    print("Mapping players...")
    player_map = {p.player_id: k for k, p in players.items()}
    print("Mapped players")
    print("Mapping player seasons...")
    player_season_map = {
        (player_map[ps.player.player_id], ps.year): ps for ps in player_seasons
    }
    print("Mapped player seasons")

    fielding_stats = []
    catching_stats = []

    for row in next(cursor.stored_results()):
        pid = row["playerID"]
        yid = row["yearID"]
        if pid not in players:
            continue
        p = players[pid]

        if (pid, yid) not in player_season_map:
            print(f"Warning: player season for {pid}, {yid} was not found in the map")
            continue
        ps = player_season_map[(pid, yid)]

        # Create fielding stats for all players
        fielding_stats.append(
            FieldingStats(
                player_season=ps, errors=row["errors"], put_outs=row["putOuts"]
            )
        )

        # Create catching stats only if they played as catcher
        if row["isCatcher"]:
            catching_stats.append(
                CatchingStats(
                    player_season=ps,
                    passed_balls=row["passedBalls"],
                    wild_pitches=row["wildPitches"],
                    steals_allowed=row["stealsAllowed"],
                    steals_caught=row["stealsCaught"],
                )
            )

        if len(fielding_stats) >= 1000:
            FieldingStats.objects.bulk_create(fielding_stats)
            print(f"Bulk created {len(fielding_stats)} fielding stats")
            fielding_stats = []

        if len(catching_stats) >= 1000:
            CatchingStats.objects.bulk_create(catching_stats)
            print(f"Bulk created {len(catching_stats)} catching stats")
            catching_stats = []

    if fielding_stats:
        FieldingStats.objects.bulk_create(fielding_stats)
        print(f"Bulk created final {len(fielding_stats)} fielding stats")

    if catching_stats:
        CatchingStats.objects.bulk_create(catching_stats)
        print(f"Bulk created final {len(catching_stats)} catching stats")

    cursor.close()
    conn.close()


def add_pitching_stats(players):
    conn = connect_to_original_db()
    cursor = conn.cursor(dictionary=True)

    # Query to get discover info each player season
    cursor.callproc("addPitchingStats")

    print("Getting player seasons...")
    player_seasons = PlayerSeason.objects.select_related("player").all()
    print("Got player seasons")
    print("Mapping players...")
    player_map = {p.player_id: k for k, p in players.items()}
    print("Mapped players")
    print("Mapping player seasons...")
    player_season_map = {
        (player_map[ps.player.player_id], ps.year): ps for ps in player_seasons
    }
    print("Mapped player seasons")

    pitching_stats = []

    for row in next(cursor.stored_results()):
        pid = row["playerID"]
        yid = row["yearID"]

        if pid not in players:
            continue
        p = players[pid]

        if (pid, yid) not in player_season_map:
            print(f"Warning: player season for {pid}, {yid} was not found in the map")
        ps = player_season_map[(pid, yid)]
        pitching_stats.append(
            PitchingStats(
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
        )

        if len(pitching_stats) >= 1000:
            PitchingStats.objects.bulk_create(pitching_stats)
            print(f"Bulk created {len(pitching_stats)} pitching stats")
            pitching_stats = []

    if pitching_stats:
        PitchingStats.objects.bulk_create(pitching_stats)
        print(f"Bulk created final {len(pitching_stats)} pitching stats")

    cursor.close()
    conn.close()


# Main function
if __name__ == "__main__":
    start_time = time.time()

    players = retrieve_players()
    teams = retrieve_teams()
    add_seasons(players, teams)
    add_batting_stats(players)
    add_fielding_stats(players)
    add_pitching_stats(players)
    # persist all the objects

    end_time = time.time()
    duration = end_time - start_time
    print(f"Conversion took {duration:.2f} seconds")
