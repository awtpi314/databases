from django import template
from django.urls import reverse
from django.utils.safestring import mark_safe

register = template.Library()

@register.filter
def filter_team_seasons(team_seasons, year):
    team_links = []
    for ts in team_seasons:
        if ts.year == year:
            url = reverse('team_details', args=[ts.team.team_id])
            link = f'<a href="{url}">{ts.team.name}</a>'
            team_links.append(link)
    return mark_safe(", ".join(team_links))

@register.filter
def get_season(player, year):
    return player.seasons.filter(year=year).first()

@register.filter
def divide(value, arg):
    try:
        return float(value) / float(arg)
    except (ValueError, ZeroDivisionError):
        return 0

@register.filter
def total_salary(players, year):
    total = 0
    for player in players:
        player_season = player.seasons.filter(year=year).first()
        if player_season and player_season.salary:
            total += player_season.salary
    return total
