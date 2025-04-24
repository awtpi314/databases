from django.http import HttpResponse, JsonResponse
from django.template import loader
from django.views.decorators.csrf import csrf_exempt

from .models import BattingStats, PitchingStats, Player, PlayerSeason, Team, TeamSeason


def mlb_data(request):
    template = loader.get_template("index.html")
    return HttpResponse(template.render())


@csrf_exempt
def player_search(request):
    template = loader.get_template("player_search.html")
    return HttpResponse(template.render())


@csrf_exempt
def player_search_results(request):
    q_name = request.POST.get("q_name")
    if q_name:
        players = Player.objects.filter(name__icontains=q_name)
    else:
        players = []
    template = loader.get_template("player_search_results.html")
    context = {"players": players, "q_name": q_name}
    return HttpResponse(template.render(context, request))


@csrf_exempt
def player_details(request, player_id):
    player = Player.objects.prefetch_related(
        "seasons__batting_stats", "positions", "team_seasons__team"
    ).get(player_id=player_id)
    template = loader.get_template("player_details.html")
    team_seasons = {}
    for ts in player.team_seasons.all():
        if ts.year not in team_seasons:
            team_seasons[ts.year] = {"name": ts.team.name, "id": ts.team.id}
        else:
            team_seasons[ts.year]["name"] += f", {ts.team.name}"
    player_stats = player.seasons.all()
    context = {
        "player": player,
        "player_seasons": player_stats,
        "team_seasons": team_seasons,
    }
    return HttpResponse(template.render(context, request))


@csrf_exempt
def team_search(request):
    template = loader.get_template("team_search.html")
    return HttpResponse(template.render())


@csrf_exempt
def team_search_results(request):
    q_name = request.POST.get("q_name")
    if q_name:
        teams = Team.objects.filter(name__icontains=q_name)
    else:
        teams = []
    template = loader.get_template("team_search_results.html")
    context = {"teams": teams, "q_name": q_name}
    return HttpResponse(template.render(context, request))


@csrf_exempt
def team_details(request, team_id):
    team = Team.objects.get(id=team_id)
    team_season = TeamSeason.objects.filter(team=team)
    template = loader.get_template("team_details.html")
    context = {"team": team, "team_seasons": team_season.all()}
    return HttpResponse(template.render(context, request))


def season_stats_json(request):
    last_year = PlayerSeason.objects.all().order_by("-year").values("year").first()
    if last_year:
        batting_stats = BattingStats.objects.prefetch_related(
            "player_season__player"
        ).filter(player_season__year=last_year["year"])
        pitching_stats = PitchingStats.objects.prefetch_related(
            "player_season__player"
        ).filter(player_season__year=last_year["year"])
    else:
        batting_stats = BattingStats.objects.none()
        pitching_stats = PitchingStats.objects.none()

    top_ten_batters = sorted(
        [
            {
                "name": batting_stats[i].player_season.player.name,
                "average": "{:.3f}".format(
                    batting_stats[i].hits / batting_stats[i].at_bats
                ),
            }
            for i in range(len(batting_stats))
            if batting_stats[i].at_bats > 9
        ],
        key=lambda x: x["average"],
        reverse=True,
    )[:10]

    top_ten_home_runs = sorted(
        [
            {
                "name": batting_stats[i].player_season.player.name,
                "home_runs": batting_stats[i].home_runs,
            }
            for i in range(len(batting_stats))
        ],
        key=lambda x: x["home_runs"],
        reverse=True,
    )[:10]
    top_ten_pitchers = sorted(
        [
            {
                "name": pitching_stats[i].player_season.player.name,
                "era": "{:.2f}".format(
                    (
                        pitching_stats[i].earned_runs_allowed
                        / pitching_stats[i].outs_pitched
                        / 3
                    )
                    * 9
                ),
            }
            for i in range(len(pitching_stats))
            if pitching_stats[i].outs_pitched > 54
        ],
        key=lambda x: x["era"],
    )[:10]

    data = {
        "batting_stats": top_ten_batters,
        "home_runs": top_ten_home_runs,
        "era_stats": top_ten_pitchers,
    }

    return JsonResponse(data, safe=False)


def season_stats_json(request):
    last_year = PlayerSeason.objects.all().order_by("-year").values("year").first()
    if last_year:
        batting_stats = BattingStats.objects.prefetch_related(
            "player_season__player"
        ).filter(player_season__year=last_year["year"])
        pitching_stats = PitchingStats.objects.prefetch_related(
            "player_season__player"
        ).filter(player_season__year=last_year["year"])
    else:
        batting_stats = BattingStats.objects.none()
        pitching_stats = PitchingStats.objects.none()

    top_ten_batters = sorted(
        [
            {
                "name": batting_stats[i].player_season.player.name,
                "average": "{:.3f}".format(
                    batting_stats[i].hits / batting_stats[i].at_bats
                ),
            }
            for i in range(len(batting_stats))
            if batting_stats[i].at_bats > 9
        ],
        key=lambda x: x["average"],
        reverse=True,
    )[:10]

    top_ten_home_runs = sorted(
        [
            {
                "name": batting_stats[i].player_season.player.name,
                "home_runs": batting_stats[i].home_runs,
            }
            for i in range(len(batting_stats))
        ],
        key=lambda x: x["home_runs"],
        reverse=True,
    )[:10]
    top_ten_pitchers = sorted(
        [
            {
                "name": pitching_stats[i].player_season.player.name,
                "era": "{:.2f}".format(
                    (
                        pitching_stats[i].earned_runs_allowed
                        / pitching_stats[i].outs_pitched
                        / 3
                    )
                    * 9
                ),
            }
            for i in range(len(pitching_stats))
            if pitching_stats[i].outs_pitched > 54
        ],
        key=lambda x: x["era"],
    )[:10]

    data = {
        "batting_stats": top_ten_batters,
        "home_runs": top_ten_home_runs,
        "era_stats": top_ten_pitchers,
    }

    return JsonResponse(data, safe=False)

@csrf_exempt
def roster_details(request, team_season_id):
    roster = Player.objects.filter(team_seasons=team_season_id)
    team_season = TeamSeason.objects.get(id=team_season_id)
    team = Team.objects.get(id=team_season.team.id)
    player_season = PlayerSeason.objects.filter(player__in=roster, year=team_season.year)
    player_season_lookup = {ps.player.player_id: ps for ps in player_season}
    template = loader.get_template("roster_details.html")
    total_salary = sum([ps.salary for ps in player_season if ps.salary is not None])
    context = {"roster": roster, "players_season": player_season_lookup, "team_season": team_season, "team": team, "payroll": total_salary}
    return HttpResponse(template.render(context, request))