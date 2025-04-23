from django.db import models

class Position(models.Model):
    position_code = models.CharField(max_length=10, primary_key=True)
    class Meta:
        db_table = "position"

class Player(models.Model):
    player_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    given_name = models.CharField(max_length=255)
    birthdate = models.DateField()
    deathdate = models.DateField(null=True)
    batting_hand = models.CharField(max_length=1, null=True)
    throwing_hand = models.CharField(max_length=1, null=True)
    birth_city = models.CharField(max_length=50, null=True)
    birth_state = models.CharField(max_length=50, null=True)
    birth_country = models.CharField(max_length=50, null=True)
    first_game = models.DateField(null=True)
    last_game = models.DateField(null=True)

    positions = models.ManyToManyField("Position")

    team_seasons = models.ManyToManyField("TeamSeason")
    
    class Meta:
        db_table = "player"
        unique_together = ("name", "birthdate")


class PlayerSeason(models.Model):
    id = models.AutoField(primary_key=True)
    player = models.ForeignKey(Player, on_delete=models.CASCADE, related_name="seasons")
    year = models.IntegerField()
    games_played = models.IntegerField(null=True)
    salary = models.DecimalField(max_digits=12, decimal_places=2, null=True)

    class Meta:
        db_table = "player_season"
        unique_together = ("player", "year")


class BattingStats(models.Model):
    id = models.AutoField(primary_key=True)
    player_season = models.OneToOneField(
        PlayerSeason, on_delete=models.CASCADE, related_name="batting_stats"
    )
    at_bats = models.IntegerField(null=True)
    hits = models.IntegerField(null=True)
    doubles = models.IntegerField(null=True)
    triples = models.IntegerField(null=True)
    home_runs = models.IntegerField(null=True)
    runs_batted_in = models.IntegerField(null=True)
    strikeouts = models.IntegerField(null=True)
    walks = models.IntegerField(null=True)
    hits_by_pitch = models.IntegerField(null=True)
    intentional_walks = models.IntegerField(null=True)
    steals = models.IntegerField(null=True)
    steals_attempted = models.IntegerField(null=True)

    class Meta:
        db_table = "batting_stats"


class CatchingStats(models.Model):
    id = models.AutoField(primary_key=True)
    player_season = models.OneToOneField(
        PlayerSeason, on_delete=models.CASCADE, related_name="catching_stats"
    )
    passed_balls = models.IntegerField(null=True)
    wild_pitches = models.IntegerField(null=True)
    steals_allowed = models.IntegerField(null=True)
    steals_caught = models.IntegerField(null=True)

    class Meta:
        db_table = "catching_stats"


class FieldingStats(models.Model):
    id = models.AutoField(primary_key=True)
    player_season = models.OneToOneField(
        PlayerSeason, on_delete=models.CASCADE, related_name="fielding_stats"
    )
    errors = models.IntegerField(null=True)
    put_outs = models.IntegerField(null=True)

    class Meta:
        db_table = "fielding_stats"


class PitchingStats(models.Model):
    id = models.AutoField(primary_key=True)
    player_season = models.OneToOneField(
        PlayerSeason, on_delete=models.CASCADE, related_name="pitching_stats"
    )
    outs_pitched = models.IntegerField(null=True)
    earned_runs_allowed = models.IntegerField(null=True)
    home_runs_allowed = models.IntegerField(null=True)
    strikeouts = models.IntegerField(null=True)
    walks = models.IntegerField(null=True)
    wins = models.IntegerField(null=True)
    losses = models.IntegerField(null=True)
    wild_pitches = models.IntegerField(null=True)
    batters_faced = models.IntegerField(null=True)
    hit_batters = models.IntegerField(null=True)
    saves = models.IntegerField(null=True)

    class Meta:
        db_table = "pitching_stats"


class Team(models.Model):
    id = models.AutoField(primary_key=True)
    old_id = models.CharField(max_length=10, null=True)
    name = models.CharField(max_length=33)
    league = models.CharField(max_length=10)
    year_founded = models.IntegerField()
    most_recent_year = models.IntegerField()


class TeamSeason(models.Model):
    id = models.AutoField(primary_key=True)
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="seasons")
    year = models.IntegerField()
    wins = models.IntegerField(null=True)
    losses = models.IntegerField(null=True)
    games_played = models.IntegerField(null=True)
    rank = models.IntegerField(null=True)
    total_attendance = models.IntegerField(null=True)

    class Meta:
        db_table = "team_season"
        unique_together = ("team", "year")
