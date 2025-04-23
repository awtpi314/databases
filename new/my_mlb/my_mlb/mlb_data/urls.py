from django.urls import path
from . import views

urlpatterns = [
    path('', views.mlb_data, name='mlb_data'),
    path('player_search/', views.player_search, name='player_search'),
    path('player_search_results/', views.player_search_results, name='player_search_results'),
    path('player/<int:player_id>/', views.player_details, name='player_details'),
]
