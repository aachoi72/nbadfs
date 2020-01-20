import requests
import re
import time
from Player import Player
from bs4 import BeautifulSoup, Comment
from pymongo import MongoClient

playerAggregateInfo = {}

client = MongoClient()
db = client.test
dateFrom = (2018, 12, 20)
# db.nbaGameLogs.delete_many({'year': 2019, 'month'})
# db.nbaPlayers.delete_many({})

def SaveGameLogs(url, player, position):
	response = requests.get(url)
	html = response.content
	soup = BeautifulSoup(html, 'html.parser')

	games = soup.find_all('th',attrs={'scope': 'row'})

	for game in games:
		gameContents = game.parent.contents
		gameInfo = {}
		for column in gameContents:
			gameInfo[column['data-stat']] = column.string

		if 'reason' in gameInfo:
			continue

		gameInfo['homeAway'] = 'home' if gameInfo['game_location'] is None else 'away'
		minutesPlayed = float(gameInfo['mp'].split(':')[0])
		decimalMinutes = float(gameInfo['mp'].split(':')[1]) / 60

		minutesPlayed += decimalMinutes
		gameInfo['mp'] = minutesPlayed

		calendarDate = gameInfo['date_game'].split('-')
		year = int(calendarDate[0])
		month = int(calendarDate[1])
		date = int(calendarDate[2])

		db.nbaGameLogs.insert_one(
			{
				"name": player,
				"year": year,
				"month": month,
				"date": date
			})

		for stat in gameInfo.keys():
			db.nbaGameLogs.update_one(
				{"name": player, "year": year, "month": month, "date": date},
				{
					"$set": {stat: gameInfo[stat]}
				})	

def SavePlayer(player, seasonDictionary, season):
	playerContents = player.contents
	playerInfo = {}
	for column in playerContents:
		playerInfo[column['data-stat']] = column.string
		if column['data-stat'] == 'player':
			playerLink = column.find_next(href=True)["href"][11:-5]

	if playerInfo['player'] not in seasonDictionary[season] or playerInfo['team_id'] == 'TOT':
		seasonDictionary[season][playerInfo['player']] = playerInfo

	url = "http://www.basketball-reference.com/players/" + playerInfo['player'].split(' ')[1][:1] + "/" + playerLink + "/gamelog/" + season
	print(url)
	SaveGameLogs(url, playerInfo['player'], playerInfo['pos'])

def SaveToMongoDB():
	for season in playerAggregateInfo.keys():
		for player in playerAggregateInfo[season].keys():
			print(player, season)
			result = db.nbaPlayers.find_one({"season": season,
						"name": player}
						)
			if result is None:
				db.nbaPlayers.insert_one(
					{
						"season": season,
						"name": player,
						"current": {
							"name": player
						}
					})
			for stat in playerAggregateInfo[season][player].keys():
				key = stat
				db.nbaPlayers.update_one(
					{"name": player, "season": season},
					{"$set": {key: playerAggregateInfo[season][player][stat]}
					})


url = "http://www.basketball-reference.com/leagues/NBA_2020_per_game.html"

response = requests.get(url)
html = response.content

soup = BeautifulSoup(html, 'html.parser')

players = soup.find_all("tr",attrs={'class': 'full_table'})
playerAggregateInfo['2020'] = {}

for player in players:
	SavePlayer(player, playerAggregateInfo, '2020')

# url = "http://www.basketball-reference.com/leagues/NBA_2018_per_game.html"

# response = requests.get(url)
# html = response.content

# soup = BeautifulSoup(html, 'html.parser')

# players = soup.find_all("tr",attrs={'class': 'full_table'})

# playerAggregateInfo['2018'] = {}
# for player in players:
# 	SavePlayer(player, playerAggregateInfo, '2018')

# url = "http://www.basketball-reference.com/leagues/NBA_2019_advanced.html"

# response = requests.get(url)
# html = response.content

# soup = BeautifulSoup(html, 'html.parser')

# players = soup.find_all("tr",attrs={'class': 'full_table'})
# playerAggregateInfo['2019'] = {}

# for player in players:
# 	SavePlayer(player, playerAggregateInfo, '2019')

# url = "http://www.basketball-reference.com/leagues/NBA_2018_advanced.html"

# response = requests.get(url)
# html = response.content

# soup = BeautifulSoup(html, 'html.parser')

# players = soup.find_all("tr",attrs={'class': 'full_table'})

# test = soup.find("td", attrs={'csk': 'Payton,Elfrid'})

# players = test.find_all_next("tr",attrs={'class': 'full_table'})

# playerAggregateInfo['2018'] = {}
# for player in players:
# 	SavePlayer(player, playerAggregateInfo, '2018')

# url = "http://www.basketball-reference.com/leagues/NBA_2020_per_minute.html"

# response = requests.get(url)
# html = response.content

# soup = BeautifulSoup(html, 'html.parser')

# players = soup.find_all("tr",attrs={'class': 'full_table'})

# playerAggregateInfo['2020'] = {}
# for player in players:
# 	SavePlayer(player, playerAggregateInfo, '2020')

# url = "http://www.basketball-reference.com/leagues/NBA_2018_per_minute.html"

# response = requests.get(url)
# html = response.content

# soup = BeautifulSoup(html, 'html.parser')

# players = soup.find_all("tr",attrs={'class': 'full_table'})

# playerAggregateInfo['2018'] = {}
# for player in players:
# 	SavePlayer(player, playerAggregateInfo, '2018')

SaveToMongoDB()