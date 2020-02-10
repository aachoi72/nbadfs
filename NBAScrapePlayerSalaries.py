import requests
import re
import time
import csv
import NBATranslations as tr
from Player import Player
from bs4 import BeautifulSoup
from pymongo import MongoClient
from collections import defaultdict
from collections import namedtuple

def ParseSiteInformation(site, prices, positions, salaryName, positionName):
	url = str.format('http://rotoguru1.com/cgi-bin/hyday.pl?mon={0}&day={1}&year={2}&game={3}', result['month'], result['date'], result['year'], site)
	print(url)
	response = requests.get(url)
	html = response.content
	soup = BeautifulSoup(html, 'html.parser')

	playersTable = soup.find('table', attrs={'cellspacing': '5'})

	players = playersTable.find_all_next(href=True, attrs={'target': '_blank'})
	playerPrices = {}
	playerPositions = {}
	playerOpponents = {}
	playerHomeAways = {}

	for player in players:
		if player.string is None:
			continue
		nameSplit = player.string.split(',')
		name = str.format('{0} {1}', nameSplit[1].strip(), nameSplit[0])

		playerContents = player.parent.parent.contents

		if playerContents[0].string == 'NA':
			continue
		playerSalary = playerContents[3].string
		playerPosition = playerContents[0].string
		playerOpponent = playerContents[5].string.split(' ')[1]
		playerHomeAway = 'away' if playerContents[5].string.split(' ')[0] == '@' else 'home'

		salary = int(playerSalary.replace('$', '').replace(',', ''))

		if name in nameTranslations:
			name = nameTranslations[name]
		playerPrices[name] = salary
		playerPositions[name] = playerPosition
		playerOpponents[name] = tr.salaryTeamTranslation[playerOpponent.upper()] if playerOpponent.upper() in tr.salaryTeamTranslation else playerOpponent.upper()
		playerHomeAways[name] = playerHomeAway

	prices[date] = playerPrices
	positions[date] = playerPositions
	dateOpponents[date] = playerOpponents
	dateHomeAways[date] = playerHomeAways

	if result['name'] not in playerPrices:
		print("Failed to save", site, date, result['name'])
		return

	db.nbaGameLogs.update_one(
		{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
		{
			'$set': {salaryName: playerPrices[result['name']]}
		})

	db.nbaGameLogs.update_one(
		{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
		{
			'$set': {positionName: playerPositions[result['name']]}
		})

	db.nbaGameLogs.update_one(
		{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
		{
			'$set': {'opp_id': playerOpponents[result['name']]}
		})

	db.nbaGameLogs.update_one(
		{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
		{
			'$set': {'homeAway': playerHomeAways[result['name']]}
		})


client = MongoClient()
db = client.test

results = db.nbaGameLogs.find({'year': 2020, 'month': 2, 'date': {'$gt': 8}})

dateFDPrices = {}
dateFDPositions = {}
dateDKPrices = {}
dateDKPositions = {}
dateOpponents = {}
dateHomeAways = {}

nameTranslations = rev = {v: k for k, v in tr.playerTranslation.items()}

for result in results:
	date = (result['year'], result['month'], result['date'])

	if date in dateFDPrices and date in dateDKPrices:
		if result['name'] not in dateFDPrices[date]:
			print("Failed to save cached FD", date, result['name'])
			continue

		if result['name'] not in dateDKPrices[date]:
			print("Failed to save cached DK", date, result['name'])
			continue

		fdPrice = dateFDPrices[date][result['name']]
		fdPositions = dateFDPositions[date][result['name']]
		dkPrice = dateDKPrices[date][result['name']]
		dkPositions = dateDKPositions[date][result['name']]
		opponents = dateOpponents[date][result['name']]
		homeAway = dateHomeAways[date][result['name']]
		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'salary': fdPrice}
			})

		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'fdPosition': fdPositions}
			})

		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'dkSalary': dkPrice}
			})

		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'dkPosition': dkPositions}
			})

		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'opp_id': opponents}
			})

		db.nbaGameLogs.update_one(
			{'name': result['name'], 'year': result['year'], 'month': result['month'], 'date': result['date']},
			{
				'$set': {'homeAway': homeAway}
			})

	else:
		ParseSiteInformation('dk', dateDKPrices, dateDKPositions,'dkSalary', 'dkPosition')
		ParseSiteInformation('fd', dateFDPrices, dateFDPositions,'salary', 'fdPosition')
