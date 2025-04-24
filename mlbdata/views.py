from django.http import HttpResponse
from django.template import loader
from django.views.decorators.csrf import csrf_exempt

from .models import Player, PlayerSeason, Team, TeamSeason


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
    player = Player.objects.prefetch_related("seasons__batting_stats", "positions", "team_seasons__team").get(player_id=player_id)
    template = loader.get_template("player_details.html")
    team_seasons = {ts.year: {"name": ts.team.name, "id": ts.team.id} for ts in player.team_seasons.all()}
    player_stats = player.seasons.all()
    context = {"player": player, "player_seasons": player_stats, "team_seasons": team_seasons}
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

@csrf_exempt
def roster_details(request, team_season_id):
    roster = Player.objects.filter(team_seasons=team_season_id)
    player_season = PlayerSeason.objects.filter(player__in=roster)
    team_season = TeamSeason.objects.get(id=team_season_id)
    team = Team.objects.get(id=team_season.team.id)
    template = loader.get_template("roster_details.html")
    total_salary = sum([ps.salary for ps in player_season if ps.salary is not None])
    context = {"roster": roster, "player_season": player_season, "team_season": team_season, "team": team, "payroll": total_salary}
    return HttpResponse(template.render(context, request))