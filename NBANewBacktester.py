from pymongo import MongoClient
from NBADataAnalyzer import NBADataAnalyzer
import pandas as pd
import NBATranslations as tr
import sys, traceback

MONTH_TO_DATE = {
1: 31,
2: 28,
3: 31,
4: 30,
10: 31,
11: 30,
12: 31
}

client = MongoClient()
db = client.test
totalPoints = []
optimisticPoints = []
pesimisticPoints = []
totalDKPoints = []
optimisticDKPoints = []
pesimisticDKPoints = []
highestPoint = 0
highestDKPoint = 0
highestDate = (0, 0, 0)
highestDKDate = (0, 0, 0)
category = 'none'
counter = 0
injuredPlayerList = []
optimisticOver300 = 0
averageOver300 = 0
pessimisticOver300 = 0
optimisticDKOver300 = 0
averageDKOver300 = 0
pessimisticDKOver300 = 0

def BacktestDate(df, currentDate):
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

    playersDF['ft_pct'] = playersDF.apply(lambda row: row.ftm_per_g / row.fta_per_g if row.fta_per_g > 0 else 0, axis=1)

    try:
        analyzer = NBADataAnalyzer(currentDate, injuredPlayerList, True)
        currentDayDF = pd.DataFrame(list(db.nbaGameLogs.find({"year": currentDate[0], "month": currentDate[1], "date": currentDate[2]}))).reset_index()
        currentDayDF = currentDayDF[pd.notnull(currentDayDF['dkPosition'])]
        currentDayDF = currentDayDF.replace({'name': tr.playerTranslation})
        currentDayDF = currentDayDF.set_index('name', drop=False)
        analyzer.SetupCurrentSeasonDF(playersDF, currentDayDF)
        optimalLineup, optimisticOptimalLineup, optimalPesimisticLineup, optimalDKLineup, optimisticOptimalDKLineup, optimalPesimisticDKLineup = analyzer.AnalyzeCurrentSlate(currentDayDF)

        totalPoints.append(optimalLineup.actual_fantasy_points)
        optimisticPoints.append(optimisticOptimalLineup.actual_fantasy_points)
        pesimisticPoints.append(optimalPesimisticLineup.actual_fantasy_points)

        totalDKPoints.append(optimalDKLineup.actual_fantasy_points)
        optimisticDKPoints.append(optimisticOptimalDKLineup.actual_fantasy_points)
        pesimisticDKPoints.append(optimalPesimisticDKLineup.actual_fantasy_points)

        global counter
        global highestPoint
        global highestDate
        global category
        global optimisticOver300
        global averageOver300
        global pessimisticOver300

        global highestDKPoint
        global highestDKDate
        global dkCategory
        global optimisticDKOver300
        global averageDKOver300
        global pessimisticDKOver300

        optimisticOver300 = optimisticOver300 + 1 if optimisticOptimalLineup.actual_fantasy_points  > 300 else optimisticOver300
        averageOver300 = averageOver300 + 1 if optimalLineup.actual_fantasy_points  > 300 else averageOver300
        pessimisticOver300 = pessimisticOver300 + 1 if optimalPesimisticLineup.actual_fantasy_points  > 300 else pessimisticOver300

        optimisticDKOver300 = optimisticDKOver300 + 1 if optimisticOptimalDKLineup.actual_fantasy_points  > 300 else optimisticDKOver300
        averageDKOver300 = averageDKOver300 + 1 if optimalDKLineup.actual_fantasy_points  > 300 else averageDKOver300
        pessimisticDKOver300 = pessimisticDKOver300 + 1 if optimalPesimisticDKLineup.actual_fantasy_points  > 300 else pessimisticDKOver300

        counter = counter + 1
        if optimalLineup.actual_fantasy_points > highestPoint:
           highestPoint = optimalLineup.actual_fantasy_points
           highestDate = currentDate
           category = 'average'

        if optimisticOptimalLineup.actual_fantasy_points > highestPoint:
            highestPoint = optimisticOptimalLineup.actual_fantasy_points
            highestDate = currentDate
            category = 'optimistic'

        if optimalPesimisticLineup.actual_fantasy_points > highestPoint:
            highestPoint = optimalPesimisticLineup.actual_fantasy_points
            highestDate = currentDate
            category = 'pesimistic'

        if optimalDKLineup.actual_fantasy_points > highestDKPoint:
           highestDKPoint = optimalDKLineup.actual_fantasy_points
           highestDKDate = currentDate
           dkCategory = 'average'

        if optimisticOptimalDKLineup.actual_fantasy_points > highestDKPoint:
            highestDKPoint = optimisticOptimalDKLineup.actual_fantasy_points
            highestDKDate = currentDate
            dkCategory = 'optimistic'

        if optimalPesimisticDKLineup.actual_fantasy_points > highestDKPoint:
            highestDKPoint = optimalPesimisticDKLineup.actual_fantasy_points
            highestDKDate = currentDate
            dkCategory = 'pesimistic'

        print('Done for FD: {0}\n\nOptimisticTotal: {2}\nAverageTotal: {1}\nPesimisticTotal: {3}\n'.format(currentDate, optimalLineup.actual_fantasy_points, optimisticOptimalLineup.actual_fantasy_points, optimalPesimisticLineup.actual_fantasy_points))
        print('Done for DK: {0}\n\nOptimisticTotal: {2}\nAverageTotal: {1}\nPesimisticTotal: {3}\n'.format(currentDate, optimalDKLineup.actual_fantasy_points, optimisticOptimalDKLineup.actual_fantasy_points, optimalPesimisticDKLineup.actual_fantasy_points))

        print('Running Average of FD OptimisticTotal: {0}'.format(sum(optimisticPoints) / counter))
        print('Running Average of FD AverageTotal: {0}'.format(sum(totalPoints) / counter))
        print('Running Average of FD PesimisticTotal: {0}'.format(sum(pesimisticPoints) / counter))
        print('Number of FD Optimistic Points Above 300: {0}'.format(optimisticOver300))
        print('Number of FD Average Points Above 300: {0}'.format(averageOver300))
        print('Number of FD Pessimistic Points Above 300: {0}'.format(pessimisticOver300))
        print('Highest FD Score: {0}. Date: {1}. Category: {2}\n'.format(highestPoint, highestDate, category))

        print('Running Average of DK OptimisticTotal: {0}'.format(sum(optimisticDKPoints) / counter))
        print('Running Average of DK AverageTotal: {0}'.format(sum(totalDKPoints) / counter))
        print('Running Average of DK PesimisticTotal: {0}'.format(sum(pesimisticDKPoints) / counter))
        print('Number of DK Optimistic Points Above 300: {0}'.format(optimisticDKOver300))
        print('Number of DK Average Points Above 300: {0}'.format(averageDKOver300))
        print('Number of DK Pessimistic Points Above 300: {0}'.format(pessimisticDKOver300))
        print('Highest DK Score: {0}. Date: {1}. Category: {2}\n'.format(highestDKPoint, highestDKDate, dkCategory))
    except Exception as err:
        print('Error with Date: ', currentDate, err)
        traceback.print_exc(file=sys.stdout)

for year in [2018, 2019]:
    for month in range(1, 13):
        if year == 2018 and month < 13:
            continue
        if year == 2019 and month > 1:
            continue
        for date in range(1, 32):
            if month == 1 and date > 21:
                continue
            if date > MONTH_TO_DATE[month]:
                continue
            if month > 9:
                gamesDF = pd.DataFrame(list(db.nbaGameLogs.aggregate([
                {
                    "$match" : {
                        "$or" : [
                                {"year": {"$eq" : year},
                                 "month" : {"$gt" : 9, "$lt": month}},
                                {"year" : {"$eq" : date},
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
                                 {"year" : {"$eq" : date},
                                 "month" : {"$eq" : month},
                                 "date"  : {"$lt" : date}},
                                {"year" : {"$eq" : year-1},
                                 "month" : {"$gt" : 9}}
                        ]
                    }
                }
                ])))
            currentDate = (year, month, date)
            BacktestDate(gamesDF, currentDate)

averageDF = pd.DataFrame(totalPoints)
optimisticDF = pd.DataFrame(optimisticPoints)

print('Average: ', averageDF.mean())
print('Optimistic: ', optimisticDF.mean())