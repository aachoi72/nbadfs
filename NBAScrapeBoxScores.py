import requests
import re
from datetime import datetime
from Player import Player
from bs4 import BeautifulSoup, Comment
from pymongo import MongoClient

client = MongoClient()
db = client.test
dateFrom = datetime(2020, 2, 7)
dateTo = datetime(2020, 2, 9)

url = "https://www.basketball-reference.com/leagues/NBA_2020_games-february.html"

response = requests.get(url)
html = response.content

soup = BeautifulSoup(html, 'html.parser')

games = soup.find_all("th",attrs={'scope': 'row'})

for game in games:
	seenPlayers = []
	date = game['csk'][:8]
	gameDate = datetime.strptime(date, '%Y%m%d')
	if gameDate > dateFrom and gameDate < dateTo:
		year = gameDate.year
		month = gameDate.month
		date = gameDate.day
		boxScoreTag = game.find_next('td', attrs={'data-stat': 'box_score_text'})
		link = boxScoreTag.find_next(href=True)
		gameUrl = 'https://www.basketball-reference.com{0}'.format(link.get('href'))

		print(gameUrl)

		response = requests.get(gameUrl)
		html = response.content
		soup = BeautifulSoup(html, 'html.parser')

		players = soup.find_all("th",attrs={'scope': 'row'})

		for player in players:
			gameContents = player.parent.contents
			gameInfo = {}
			for column in gameContents:
				gameInfo[column['data-stat']] = column.string

			if 'reason' in gameInfo or gameInfo['player'] == 'Team Totals' or gameInfo['player'] in seenPlayers:
				continue

			# gameInfo['homeAway'] = 'home' if gameInfo['game_location'] is None else 'away'
			minutesPlayed = float(gameInfo['mp'].split(':')[0])
			decimalMinutes = float(gameInfo['mp'].split(':')[1]) / 60

			minutesPlayed += decimalMinutes
			gameInfo['mp'] = minutesPlayed

			result = db.nbaGameLogs.find_one({'year': year, 'month': month, 'date': date, 'name': gameInfo['player']})
			if result is None:
				print(gameInfo)
				db.nbaGameLogs.insert_one(
					{
						"name": gameInfo['player'],
						"year": year,
						"month": month,
						"date": date
					})

				for stat in gameInfo.keys():
					db.nbaGameLogs.update_one(
						{"name": gameInfo['player'], "year": year, "month": month, "date": date},
						{
							"$set": {stat: gameInfo[stat]}
						})

			seenPlayers.append(gameInfo['player'])

			# print(gameInfo)

