from pymongo import MongoClient
from NBADataAnalyzer import NBADataAnalyzer
import pandas as pd
import sys, traceback
import NBATranslations as tr

client = MongoClient()
db = client.test
totalPoints = []
optimisticPoints = []
pesimisticPoints = []
totalDKPoints = []
optimisticDKPoints = []
pesimisticDKPoints = []
highestPoint = 0
highestDate = (0, 0, 0)
category = 'none'
counter = 0
year = 2020
month = 1
date = 20
injuredPlayerList = []

def BacktestDate(df, currentDate, currentDayDF, isBacktest):
    gamesDF['pts_per_g'] = gamesDF.apply(lambda row: float(row.pts), axis=1)
    gamesDF['fg2_per_g'] = gamesDF.apply(lambda row: float(row.fg) - float(row.fg3), axis=1)
    gamesDF['fg3_per_g'] = gamesDF.apply(lambda row: float(row.fg3), axis=1)
    gamesDF['fta_per_g'] = gamesDF.apply(lambda row: float(row.fta), axis=1)
    gamesDF['ftm_per_g'] = gamesDF.apply(lambda row: float(row.ft), axis=1)
    gamesDF['ast_per_g'] = gamesDF.apply(lambda row: float(row.ast), axis=1)
    gamesDF['orb_per_g'] = gamesDF.apply(lambda row: float(row.orb), axis=1)
    gamesDF['drb_per_g'] = gamesDF.apply(lambda row: float(row.drb), axis=1)
    gamesDF['stl_per_g'] = gamesDF.apply(lambda row: float(row.stl), axis=1)
    gamesDF['blk_per_g'] = gamesDF.apply(lambda row: float(row.blk), axis=1)
    gamesDF['tov_per_g'] = gamesDF.apply(lambda row: float(row.tov), axis=1)
    gamesDF['mp_per_g'] = gamesDF.apply(lambda row: float(row.mp), axis=1)

    playersDF = gamesDF.groupby(['name']).mean().reset_index()

    playersDF['ft_pct'] = playersDF.apply(lambda row: row.ftm_per_g / row.fta_per_g if row.fta_per_g > 0 else 0.0, axis=1)

    try:
        analyzer = NBADataAnalyzer(currentDate, injuredPlayerList, isBacktest)
        # print(currentDayDF)
        analyzer.SetupCurrentSeasonDF(playersDF, currentDayDF)
        optimalLineup, optimisticOptimalLineup, optimalPesimisticLineup, optimalDKLineup, optimisticOptimalDKLineup, optimalPesimisticDKLineup = analyzer.AnalyzeCurrentSlate(currentDayDF)

        totalPoints.append(optimalLineup.actual_fantasy_points)
        optimisticPoints.append(optimisticOptimalLineup.actual_fantasy_points)
        pesimisticPoints.append(optimalPesimisticLineup.actual_fantasy_points)

        totalDKPoints.append(optimalDKLineup.actual_fantasy_points)
        optimisticDKPoints.append(optimisticOptimalDKLineup.actual_fantasy_points)
        pesimisticDKPoints.append(optimalPesimisticDKLineup.actual_fantasy_points)

        print('Done for: {0}\n\nAverageTotal: {1}\nOptimisticTotal: {2}\nPesimisticTotal: {3}\n'.format(currentDate, optimalLineup.actual_fantasy_points, optimisticOptimalLineup.actual_fantasy_points, optimalPesimisticLineup.actual_fantasy_points))

        print('Optimistic Lineup')
        print(optimisticOptimalLineup)
        print('Total: ', optimisticOptimalLineup.actual_fantasy_points)
        print('=====================================================')

        print('Average Lineup')
        print(optimalLineup)
        print('Total: ', optimalLineup.actual_fantasy_points)
        print('=====================================================')

        print('Pestimistic Lineup')
        print(optimalPesimisticLineup)
        print('Total: ', optimalPesimisticLineup.actual_fantasy_points)

    except Exception as err:
        print('Error with Date: ', currentDate, err)
        traceback.print_exc(file=sys.stdout)

if month > 9:
    gamesDF = pd.DataFrame(list(db.nbaGameLogs.aggregate([
    {
        "$match" : {
            "$or" : [
                    {"year": {"$eq" : year},
                     "month" : {"$gt" : 9, "$lt": month}},
                    {"year" : {"$eq" : year},
                     "month" : {"$eq" : month},
                     "date" : {"$lt" : date}}
            ]
        }
    }
    ])))
else:
    gamesDF = pd.DataFrame(list(db.nbaGameLogs.aggregate([
    {
        "$match" : {
            "$or" : [
                    {"year": {"$eq" : year},
                     "month" : {"$lt" : month}},
                     {"year" : {"$eq" : year},
                     "month" : {"$eq" : month},
                     "date"  : {"$lt" : date}},
                    {"year" : {"$eq" : year-1},
                     "month" : {"$gt" : 9}}
            ]
        }
    }
    ])))
currentDate = (year, month, date)

if len(sys.argv) == 1:
    currentDayDF = pd.DataFrame(list(db.nbaGameLogs.find({"year": currentDate[0], "month": currentDate[1], "date": currentDate[2]}))).reset_index()
    currentDayDF = currentDayDF[pd.notnull(currentDayDF['dkPosition'])]
    currentDayDF = currentDayDF.replace({'name': tr.playerTranslation})
    currentDayDF = currentDayDF.set_index('name', drop=False)
    isBacktest = True
else:
    FDfileName = sys.argv[1]
    isBacktest = False
    currentDayDF = pd.read_csv(FDfileName)
    currentDayDF.rename(columns = {'Nickname':'name', 'Position':'fdPosition', 'Opponent':'opp_id', 'Salary':'salary'}, inplace = True)
    currentDayDF = currentDayDF.replace({'name': tr.fanduelNameTranslation})
    currentDayDF = currentDayDF.replace({'opp_id' : tr.fanduelTeamTranslation})
    currentDayDF['homeAway'] = currentDayDF.apply(lambda row: 'home' if row.Game.split('@')[1] == row.Team else 'away', axis=1)
    currentDayDF = currentDayDF.set_index('name', drop=False)

    if len(sys.argv) > 2:
        DKFileName = sys.argv[2]
        currentDayDKDF = pd.read_csv(DKFileName)
        currentDayDKDF = currentDayDKDF.replace({'Name': tr.draftkingsNameTranslation})
        currentDayDKDF = currentDayDKDF.set_index('Name', drop=False)
        currentDayDF['dkPosition'] = currentDayDF.apply(lambda row: currentDayDKDF.at[row.name, 'Position'] if row.name in currentDayDKDF.index else row.fdPosition, axis=1)
        currentDayDF['dkSalary'] = currentDayDF.apply(lambda row: currentDayDKDF.at[row.name, 'Salary'] if row.name in currentDayDKDF.index else row.salary, axis=1)
    else:
        currentDayDF['dkPosition'] = currentDayDF['fdPosition']
        currentDayDF['dkSalary'] = currentDayDF['salary']

currentDayDF = currentDayDF.set_index('name', drop=False)

BacktestDate(gamesDF, currentDate, currentDayDF, isBacktest)

averageDF = pd.DataFrame(totalPoints)
optimisticDF = pd.DataFrame(optimisticPoints)

print('Average: ', averageDF.mean())
print('Optimistic: ', optimisticDF.mean())