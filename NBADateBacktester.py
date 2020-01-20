import requests
import re
import time
import csv
import copy
import re
import pandas as pd
from Player import Player
from bs4 import BeautifulSoup
from pymongo import MongoClient
from collections import defaultdict
from collections import namedtuple
from operator import itemgetter
from pulp import *

client = MongoClient()
db = client.test

date = (2018, 12, 18)
recentDate = (2018, 11, 25)

injuredPlayers = []

maxPPDPlayersToConsider = 10
threeInFourRestMultiplier = 0.85
threeInFourMultiplier = 0.5
backToBackMultiplier = 0.75

dvpMultiplier = 2
opponentMultiplier = 0.25
homeAwayMultiplier = 0.5
recentMultiplier = 1
adjCeiling = 3

playerTranslations = {
    'Dennis Smith Jr.' : "Dennis Smith",
    'Derrick Jones Jr.' : 'Derrick Jones',
    'Larry Nance Jr.' : 'Larry Nance',
    'Tim Hardaway Jr.' : 'Tim Hardaway',
    'Jaren Jackson Jr.' : 'Jaren Jackson',
    'Marvin Bagley III' : 'Marvin Bagley',
    'Wendell Carter Jr.' : 'Wendell Carter',
    'Robert Williams III' : 'Robert Williams',
    'Harry Giles III' : 'Harry Giles',
    'Taurean Prince' : 'Taurean Waller-Prince',
    'CJ Miles' : 'C.J. Miles',
    'TJ Leaf' : 'T.J. Leaf',
    'Glenn Robinson III' : 'Glenn Robinson',
    'Wes Iwundu' : 'Wesley Iwundu'
    }

finalTranslations = {
    'Karl Anthony Towns' : 'Karl-Anthony Towns',
    'Rondae Hollis Jefferson' : 'Rondae Hollis-Jefferson',
    'Al Farouq Aminu' : 'Al-Farouq Aminu',
    'Michael Kidd Gilchrist' : 'Michael Kidd-Gilchrist',
    'Shai Gilgeous Alexander' : 'Shai Gilgeous-Alexander'
}

teamTranslations = {
    "ATL": "ATL",
    "BKN": "BRK",
    "BOS": "BOS",
    "CHA": "CHO",
    "CHI": "CHI",
    "CLE": "CLE",
    "DAL": "DAL",
    "DEN": "DEN",
    "DET": "DET",
    "HOU": "HOU",
    "GS" : "GSW",
    "IND": "IND",
    "LAC": "LAC",
    "LAL": "LAL",
    "MEM": "MEM",
    "MIA": "MIA",
    "MIL": "MIL",
    "MIN": "MIN",
    "NO" : "NOP",
    "NY" : "NYK",
    "OKC": "OKC",
    "ORL": "ORL",
    "PHI": "PHI",
    "PHO": "PHO",
    "POR": "POR",
    "SA" : "SAS",
    "SAC": "SAC",
    "TOR": "TOR",
    "UTA": "UTA",
    "WSH": "WAS",
    "WAS": "WAS"
}


def CalculateAvgPoints(result):
    if 'pts_per_g' not in result:
        return 0
    return float(result['pts_per_g']) + (1.5 * float(result['ast_per_g'])) + (1.2 * float(result['trb_per_g'])) + (2 * float(result['stl_per_g'])) + (2 * float(result['blk_per_g'])) - float(result['tov_per_g'])

def CalculatePoints(result):
    return int(result['pts']) + (1.5 * int(result['ast'])) + (1.2 * int(result['trb'])) + (2 * int(result['stl'])) + (2 * int(result['blk'])) - int(result['tov'])

def Summarize(prob):
    div = '---------------------------------------\n'
    print("Variables:\n")
    score = str(prob.objective)
    constraints = [str(const) for const in prob.constraints.values()]
    for v in prob.variables():
        score = score.replace(v.name, str(v.varValue))
        constraints = [const.replace(v.name, str(v.varValue)) for const in constraints]
        if v.varValue != 0:
            print(v.name, "=", v.varValue)
    print(div)
    print("Constraints:")
    for constraint in constraints:
        constraint_pretty = " + ".join(re.findall("[0-9\.]*\*1.0", constraint))
        if constraint_pretty != "":
            print("{} = {}".format(constraint_pretty, eval(constraint_pretty)))
    print(div)
    print("Score:")
    score_pretty = " + ".join(re.findall("[0-9\.]+\*1.0", score))
    print("{} = {}".format(score_pretty, eval(score)))

def GetOptimizedLineup(players, labels):
    playerList = pd.DataFrame.from_records(players, columns=labels)

    salaries = {}
    points = {}
    for position in playerList.position.unique():
        available_pos = playerList[playerList.position == position]
        salary = list(available_pos[["name","salary"]].set_index("name").to_dict().values())[0]
        point = list(available_pos[["name","projectedPoints"]].set_index("name").to_dict().values())[0]
        salaries[position] = salary
        points[position] = point

    pos_num_available = {
    "PG": 2,
    "SG": 2,
    "SF": 2,
    "PF": 2,
    "C": 1
    }

    MAX_SALARY = 60000

    _vars= {k: LpVariable.dict(k, v, cat="Binary") for k, v in points.items()}
    problem = LpProblem("Fantasy", LpMaximize)
    rewards = []
    costs =[]
    position_constraints = []

    for k, v in _vars.items():
        costs += lpSum([salaries[k][i] * _vars[k][i] for i in v])
        rewards += lpSum([points[k][i] * _vars[k][i] for i in v])
        problem += lpSum([_vars[k][i] for i in v]) <= pos_num_available[k]

    problem += lpSum(rewards)
    problem += lpSum(costs) <= MAX_SALARY

    problem.solve()

    # Summarize(problem)

    finalTeamNames = []
    finalTeam = []
    for v in problem.variables():
        if v.varValue != 0:
            values = v.name.split('_')
            name = ' '.join(values[1:])
            name = name if name not in finalTranslations else finalTranslations[name]
            finalTeamNames.append(name)
            print(name)

    for player in players:
        if player[0] in finalTeamNames:
            finalTeam.append(player)
    return finalTeam
# def GetOptimizedLineup(players):
#     maxPosition = {}
#     maxPosition['PG'] = 2
#     maxPosition['SG'] = 2
#     maxPosition['SF'] = 2
#     maxPosition['PF'] = 2
#     maxPosition['C'] = 1

#     maxSalary=60000
#     teamPoints=0
#     totalPlayers = 9
#     minProjectedPoints = 1000
#     playersCopy = copy.deepcopy(players)

#     for position in playersCopy:
#         playersCopy[position].sort(key=itemgetter(5), reverse=True)
#         playersCopy[position] = playersCopy[position][:maxPPDPlayersToConsider]

#     combinedList = []
#     finalTeam = []
#     playersInTeam = {}

#     for playerLists in playersCopy:
#         combinedList.extend(playersCopy[playerLists])

#     combinedList.sort(key=itemgetter(5), reverse=True)

#     finalTeam.append(combinedList[0])
#     maxPosition[combinedList[0][1]] -= 1
#     totalPlayers -= 1
#     teamPoints += combinedList[0][4]
#     minProjectedPoints = combinedList[0][4]
#     maxSalary -= combinedList[0][3]
#     playersInTeam[combinedList[0][2]] = 1
#     combinedList.pop(0)

#     finalTeam.append(combinedList[0])
#     maxPosition[combinedList[0][1]] -= 1
#     totalPlayers -= 1
#     teamPoints += combinedList[0][4]
#     minProjectedPoints = combinedList[0][4] if combinedList[0][4] < minProjectedPoints else minProjectedPoints
#     maxSalary -= combinedList[0][3]
#     if combinedList[0][2] in playersInTeam:
#         playersInTeam[combinedList[0][2]] += 1
#     else:
#         playersInTeam[combinedList[0][2]] = 1

#     combinedList.pop(0)

#     while maxPosition[combinedList[0][1]] == 0:
#         combinedList.pop(0)

#     finalTeam.append(combinedList[0])
#     maxPosition[combinedList[0][1]] -= 1
#     totalPlayers -= 1
#     teamPoints += combinedList[0][4]
#     minProjectedPoints = combinedList[0][4] if combinedList[0][4] < minProjectedPoints else minProjectedPoints
#     maxSalary -= combinedList[0][3]
#     if combinedList[0][2] in playersInTeam:
#         playersInTeam[combinedList[0][2]] += 1
#     else:
#         playersInTeam[combinedList[0][2]] = 1

#     combinedList.pop(0)

#     while maxPosition[combinedList[0][1]] == 0:
#         combinedList.pop(0)

#     combinedList.sort(key=itemgetter(4), reverse=True)
#     for player in combinedList:
#         position = player[1]
#         if maxPosition[position] == 0 or (totalPlayers == 1 and maxSalary < player[3]) or (totalPlayers > 1 and ((maxSalary - player[3]) / (totalPlayers-1)) < 3500):
#             continue
#         if player[2] in playersInTeam and playersInTeam[player[2]] == 4:
#             continue
#         elif player[2] in playersInTeam:
#             playersInTeam[player[2]] += 1
#         else:
#             playersInTeam[player[2]] = 1

#         maxPosition[position] -= 1
#         maxSalary -= player[3]
#         totalPlayers -= 1
#         finalTeam.append(player)
#         teamPoints += player[4]
#         minProjectedPoints = player[4] if player[4] < minProjectedPoints else minProjectedPoints

#         if totalPlayers == 0:
#             break

#     if totalPlayers != 0:
#         for position in players:
#             players[position].sort(key=itemgetter(4), reverse=True)
#             for player in players[position]:
#                 if maxPosition[position] == 0 or totalPlayers == 0:
#                     break
#                 elif (totalPlayers == 1 and maxSalary < player[3]) or (totalPlayers > 1 and ((maxSalary - player[3]) / (totalPlayers-1)) < 3500) or player in finalTeam:
#                     continue

#                 if player[2] in playersInTeam and playersInTeam[player[2]] == 4:
#                     continue
#                 elif player[2] in playersInTeam:
#                     playersInTeam[player[2]] += 1
#                 else:
#                     playersInTeam[player[2]] = 1
#                     maxPosition[position] -= 1
#                 maxSalary -= player[3]
#                 totalPlayers -= 1
#                 finalTeam.append(player)
#                 teamPoints += player[4]
#                 minProjectedPoints = player[4] if player[4] < minProjectedPoints else minProjectedPoints

#             if totalPlayers == 0:
#                 break

#     teamPoints = teamPoints - minProjectedPoints
#     return finalTeam, teamPoints

#START OF ANALYSIS
#Overall Average Section

#2016
playerGameLogs = db.nbaGameLogs.aggregate([
    {
        "$match" : {
            "$or" : [
                    {"year" : {"$eq" : 2018},
                     "month" : {"$lt" : date[1]}},
                    {"year" : {"$eq" : 2018},
                     "month" : {"$eq" : date[1]},
                     "date" : {"$lt" : date[2]-1}}
            ]
        }
    }
])
#2017
# playerGameLogs = db.nbaGameLogs.aggregate([
#     {
#         "$match" : {
#             "$or" : [
#                     {"year" : {"$eq" : 2015}},
#                     {"year" : {"$eq" : 2016}},
#                     {"year" : {"$eq" : 2017},
#                      "month" : {"$eq" : date[1]},
#                      "date" : {"$lt" : date[2]-1}}
#             ]
#         }
#     }
# ])

playerPositionMinutes = {}

playerPoints = db.nbaPlayers.find({'season': '2019'})

for player in playerPoints:
    playerPositionMinutes[player['name']] = player['pos'].split('-')[0], 0

playerTotal = {}
playerGames = {}
playerAverage = {}

playerPerMinTotal = {}
playerPerMinGames = {}
playerPerMinAverage = {}

playerHomeAwayTotal = {'home': {}, 'away': {}}
playerHomeAwayGames = {'home': {}, 'away': {}}
playerHomeAwayAverage = {'home': {}, 'away': {}}

dvpTotal = {'PG': {}, 'SG': {}, 'SF': {}, 'PF': {}, 'C': {}}
dvpGames = {'PG': {}, 'SG': {}, 'SF': {}, 'PF': {}, 'C': {}}
dvpAverage = {'PG': {}, 'SG': {}, 'SF': {}, 'PF': {}, 'C': {}}

opponentPlayerTotal = {}
opponentPlayerGames = {}
opponentPlayerAverage = {}

for game in playerGameLogs:
    if 'mp' not in game or game['mp'] == 0:
        continue
    name = game['name']
    points = CalculatePoints(game)

    pointsPerMin = points / game['mp']
    if name in playerTotal:
        playerTotal[name] += points
        playerGames[name] += 1
        playerPerMinTotal[name] += pointsPerMin
        playerPerMinGames[name] += 1
    else:
        playerTotal[name] = points
        playerGames[name] = 1
        playerPerMinTotal[name] = pointsPerMin
        playerPerMinGames[name] = 1

for player in playerTotal:
    playerAverage[player] = playerTotal[player] / playerGames[player]
    playerPerMinAverage[player] = playerPerMinTotal[player] / playerPerMinGames[player]

#2016
#DVP/HomeAway/Opponent Section
playerGameLogs = db.nbaGameLogs.aggregate([
    {
        "$match" : {
            "$or" : [
                    {"year" : {"$eq" : 2017}},
                    {"year" : {"$eq" : 2018},
                     "month" : {"$lt" : date[1]}},
                    {"year" : {"$eq" : 2018},
                     "month" : {"$eq" : date[1]},
                     "date" : {"$lt" : date[2]-1}}
            ]}
    }]
)

#2017
# playerGameLogs = db.nbaGameLogs.aggregate([
#     {
#         "$match" : {
#             "$or" : [
#                     {"year" : {"$eq" : 2015}},
#                     {"year" : {"$eq" : 2016}},
#                     {"year" : {"$eq" : 2017},
#                      "month" : {"$eq" : date[1]},
#                      "date" : {"$lt" : date[2]-1}}
#             ]
#         }
#     }
# ])

for game in playerGameLogs:
    if 'fdPosition' not in game or game['mp'] == 0:
        continue
    name = game['name']
    if name not in playerAverage:
        print("Player not in averages: ", game)
        continue
    points = CalculatePoints(game)
    position = game['fdPosition']
    opponent = game['opp_id']
    homeAway = game['homeAway']
    home = game['opp_id'] if game['homeAway'] == 'away' else game['team_id']
    pointDifference = points - playerAverage[name]

    if opponent in dvpGames[position]:
        dvpGames[position][opponent] += 1
        dvpTotal[position][opponent] += pointDifference
    else:
        dvpGames[position][opponent] = 1
        dvpTotal[position][opponent] = pointDifference

    if name in playerHomeAwayTotal[homeAway]:
        playerHomeAwayGames[homeAway][name] += 1
        playerHomeAwayTotal[homeAway][name] += pointDifference
    else:
        playerHomeAwayGames[homeAway][name] = 1
        playerHomeAwayTotal[homeAway][name] = pointDifference

    if name not in opponentPlayerTotal:
        opponentPlayerTotal[name] = {}
        opponentPlayerGames[name] = {}
        opponentPlayerAverage[name] = {}

    if opponent in opponentPlayerTotal[name]:
        opponentPlayerGames[name][opponent] += 1
        opponentPlayerTotal[name][opponent] += pointDifference
    else:
        opponentPlayerGames[name][opponent] = 1
        opponentPlayerTotal[name][opponent] = pointDifference


for position in dvpTotal:
    for team in dvpTotal[position]:
        dvpAverage[position][team] = dvpTotal[position][team] / dvpGames[position][team]

for homeAway in playerHomeAwayTotal:
    for player in playerHomeAwayTotal[homeAway]:
        playerHomeAwayAverage[homeAway][player] = playerHomeAwayTotal[homeAway][player] / playerHomeAwayGames[homeAway][player]

for player in opponentPlayerTotal:
    for opponent in opponentPlayerTotal[player]:
        opponentPlayerAverage[player][opponent] = opponentPlayerTotal[player][opponent] / opponentPlayerGames[player][opponent]

# Recent Game Section

recentGameLogs = db.nbaGameLogs.aggregate([
    {
        "$match" : {
            "$or" : [
                    {"year" : {"$eq" : recentDate[0]},
                     "month" : {"$eq" : recentDate[1]},
                     "date" : {"$gte" : recentDate[2]}},
                    {"year" : {"$eq" : date[0]},
                     "month" : {"$eq" : date[1]},
                     "date" : {"$lt" : date[2]}}
            ]
        }
    }
])

recentPlayerTotal = {}
recentPlayerGames = {}
recentPlayerAverage = {}

minutesPlayedTotal = {}
minutesPlayedGames = {}
minutesPlayedAverage = {}

for game in recentGameLogs:
    if 'mp' not in game or game['mp'] == 0:
        continue
    name = game['name']
    points = CalculatePoints(game)
    if name in recentPlayerTotal:
        recentPlayerTotal[name] += points
        recentPlayerGames[name] += 1
        minutesPlayedTotal[name] += game['mp']
        minutesPlayedGames[name] += 1
    else:
        recentPlayerTotal[name] = points
        recentPlayerGames[name] = 1
        minutesPlayedTotal[name] = game['mp']
        minutesPlayedGames[name] = 1

for player in recentPlayerTotal:
    recentPlayerAverage[player] = recentPlayerTotal[player] / recentPlayerGames[player]
    minutesPlayedAverage[player] = minutesPlayedTotal[player] / minutesPlayedGames[player]

availablePlayerSalary = {}
playerPositionPoints = []

usageDifference = {}

#Injured Player section
for player in injuredPlayers:
    playerStats = db.nbaPlayers.find_one({'name': player,'season': '2019'})
    usage = playerStats['usg_pct']
    playersOnTeam = db.nbaPlayers.find({'season': '2019', 'team_id': playerStats['team_id']})
    for teammate in playersOnTeam:
        if teammate == player:
            continue
        teammateName = teammate['name']
        teammateUsage = teammate['usg_pct'] if 'usg_pct' in teammate else 0
        usageChange = (float(teammateUsage) / 100 * float(usage)) / 100
        # print(usageChange, teammate['name'])
        if teammate['pos'] == playerStats['pos']:
            usageChange *= 2
        usageDifference[teammateName] = usageChange
    # print(usageDifference)

Player = namedtuple('Player', 'name opponent position salary homeAway team')
players = []

# Actual CSV
# with open('FanDuelNBAPlayers.csv', 'rt') as file:
#     reader = csv.reader(file)
#     header = 0
#     for row in reader:
#         if (header == 0):
#             header += 1
#             continue
#         name = row[3]
#         if name in playerTranslations:
#             name = playerTranslations[name]
#         opponent = row[10] if row[10] not in teamTranslations else teamTranslations[row[10]]
#         position = row[1]
#         salary = int(row[7])
#         homeAway = 'home' if row[8].split('@')[0] == row[10] else 'away'
#         team = row[9] if row[9] not in teamTranslations else teamTranslations[row[9]]
#         if len(opponent) > 1:
#             players.append(Player(name=name, opponent=opponent, position=position, salary=salary, homeAway = homeAway, team=team))

# BackTest Historical Data
historicalPlayers = db.nbaGameLogs.find({
            "year": date[0],
            "month": date[1],
            "date": date[2]
    })

for player in historicalPlayers:
    if 'mp' not in player:
        continue
    try:
        players.append(Player(name=player['name'], opponent=player['opp_id'], position=player['fdPosition'], salary=player['salary'], homeAway = player['homeAway'], team=player['team_id']))
    except:
        print(player)
        throw

#Final Calc section
for player in players:
    name = player.name if player.name not in playerTranslations else playerTranslations[player.name]

    if name in injuredPlayers or name not in playerPositionMinutes:
        print("Player needs translation: ", name)
        continue

    homeAway = player.homeAway
    opponent = player.opponent
    position = player.position
    dvpPosition = playerPositionMinutes[name][0]
    averagePoints = playerAverage[name] if name in playerAverage else 0

    threeDaysPreviousResult = db.nbaGameLogs.find_one({'year': date[0], 'month': date[1], 'date': date[2]-3, 'name': name})
    twoDaysPreviousResult = db.nbaGameLogs.find_one({'year': date[0], 'month': date[1], 'date': date[2]-2, 'name': name})
    previousDayResult = db.nbaGameLogs.find_one({'year': date[0], 'month': date[1], 'date': date[2]-1, 'name': name})

    dvpPointDifference = min(dvpAverage[dvpPosition][opponent] * dvpMultiplier, adjCeiling)
    homeAwayDifference = min(playerHomeAwayAverage[homeAway][name] * homeAwayMultiplier, adjCeiling) if name in playerHomeAwayAverage[homeAway] else 0
    opponentPlayerDifference = min(opponentPlayerAverage[name][opponent] * opponentMultiplier, adjCeiling) if name in opponentPlayerAverage and opponent in opponentPlayerAverage[name] else 0
    recentDifference = recentPlayerAverage[name] if name in recentPlayerAverage else 0

    if recentDifference != 0:
        recentDifference -= averagePoints

    recentDifference *= recentMultiplier

    recentDifference = min(recentDifference, adjCeiling)

    projectedPoints = playerAverage[name] if name in playerAverage else 0 
    projectedPoints += dvpPointDifference + homeAwayDifference + recentDifference + opponentPlayerDifference

    if previousDayResult is not None and threeDaysPreviousResult is not None:
        projectedPoints *= threeInFourMultiplier
    elif threeDaysPreviousResult is not None and twoDaysPreviousResult is not None:
        projectedPoints *= threeInFourRestMultiplier
    elif previousDayResult is not None:
        projectedPoints *= backToBackMultiplier

    # if float(playerPositionMinutes[name][1]) > 10:
    #     projectedPoints = projectedPoints * minutesPlayedAverage[name] / float(playerPositionMinutes[name][1])

    # playerPositionPoints.append((name, position, player.team, player.salary, projectedPoints, projectedPoints*1000/player.salary, averagePoints, dvpPointDifference, homeAwayDifference, opponentPlayerDifference, recentDifference))
    playerPositionPoints.append((name, position, player.team, player.salary, projectedPoints, averagePoints, dvpPointDifference, homeAwayDifference, opponentPlayerDifference, recentDifference))
    
labels = ['name', 'position', 'team', 'salary', 'projectedPoints', 'averagePoints', 'dvpPointDifference', 'homeAwayDifference', 'opponentPlayerDifference', 'recentDifference']    

finalTeam = GetOptimizedLineup(playerPositionPoints, labels)
# finalTeam, projectedPoints = GetOptimizedLineup(playerPositionPoints, labels)

actualPoints = 0
projectedPoints = 0
salary = 0
minActualPoints = 1000
minProjectedPoints = 1000

#BackTesting
for player in finalTeam:
    name = player[0] if player[0] not in playerTranslations else playerTranslations[player[0]]
    result = db.nbaGameLogs.find_one({'year': date[0], 'month': date[1], 'date': date[2], 'name': player[0]})
    if result is None:
        print(name)
    playerPoints = CalculatePoints(result)
    actualPoints += playerPoints
    projectedPoints += player[4]
    if player[4] < minProjectedPoints:
        minProjectedPoints = player[4]
    salary += player[3]
    if playerPoints < minActualPoints:
        minActualPoints =playerPoints
    print(player[0], player[1], player[3], round(player[4],2), round(playerPoints, 2), "currentAverage", round(player[5], 2), "dvp", round(player[6], 2), "homeAway", round(player[7], 2), "opponent", round(player[8], 2), "recent", round(player[9], 2))

#Live Projection
# for player in finalTeam:
#     projectedPoints += player[4]
#     if player[4] < minProjectedPoints:
#         minProjectedPoints = player[4]
#     salary += player[3]
#     print(player[0], player[1], player[3], round(player[4],2), "dvp", round(player[6], 2), "homeAway", round(player[7], 2), "opponent", round(player[8], 2), "recent", round(player[9], 2))

# print(finalTeam)
print(projectedPoints - minProjectedPoints, minProjectedPoints)
print(actualPoints - minActualPoints, minActualPoints)
print(salary)