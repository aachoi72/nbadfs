import pandas as pd
import numpy as np
import NBATranslations as tr
# import NBALineupOptimizer as optimizer
from pymongo import MongoClient
from pydfs_lineup_optimizer import get_optimizer, Site, Sport, Player, CSVLineupExporter
from itertools import tee

MONTH_TO_DATE = {
1: 31,
2: 28,
3: 31,
4: 30,
10: 31,
11: 30,
12: 31
}
MAXLINEUPS = 5

class NBADataAnalyzer:
    def __init__(self, currentDate, injuredPlayerList, isBackTesting=False):
        client = MongoClient()
        self.db = client.test
        self.currentDate = currentDate
        self.injuredPlayers = injuredPlayerList
        self.threeDaysPrevious = []
        self.twoDaysPrevious = []
        self.oneDayPrevious = []
        self.FindRecentRest()
        self.isBackTesting = isBackTesting
        self.importantStats = ['pts', 'fg3', 'ast', 'orb', 'drb', 'stl', 'blk', 'tov']

    def SetupCurrentSeasonDF(self, playersDF, currentDF):
        self.playersDF = playersDF
        self.playersDF = self.playersDF[['name', 'pts_per_g', 'fg2_per_g', 'fg3_per_g', 'fta_per_g', 'ft_pct', 'ast_per_g', 'orb_per_g', 'drb_per_g', 'stl_per_g','blk_per_g', 'tov_per_g', 'mp_per_g']]
        self.playersDF = self.playersDF.replace({'name': tr.playerTranslation})
        self.playersDF['pts_per_min'] = self.playersDF.apply(lambda row: row.pts_per_g / row.mp_per_g, axis=1)
        self.playersDF['fg2_per_min'] = self.playersDF.apply(lambda row: row.fg2_per_g / row.mp_per_g, axis=1)
        self.playersDF['fg3_per_min'] = self.playersDF.apply(lambda row: row.fg3_per_g / row.mp_per_g, axis=1)
        self.playersDF['fta_per_min'] = self.playersDF.apply(lambda row: row.fta_per_g / row.mp_per_g, axis=1)
        self.playersDF['ast_per_min'] = self.playersDF.apply(lambda row: row.ast_per_g / row.mp_per_g, axis=1)
        self.playersDF['orb_per_min'] = self.playersDF.apply(lambda row: row.orb_per_g / row.mp_per_g, axis=1)
        self.playersDF['drb_per_min'] = self.playersDF.apply(lambda row: row.drb_per_g / row.mp_per_g, axis=1)
        self.playersDF['stl_per_min'] = self.playersDF.apply(lambda row: row.stl_per_g / row.mp_per_g, axis=1)
        self.playersDF['blk_per_min'] = self.playersDF.apply(lambda row: row.blk_per_g / row.mp_per_g, axis=1)
        self.playersDF['tov_per_min'] = self.playersDF.apply(lambda row: row.tov_per_g / row.mp_per_g, axis=1)

        self.playersDF = self.playersDF.set_index('name', drop=False)
        self.SetupGamesDiffPctDF(currentDF)

    def SetupGamesDiffPctDF(self, currentDF):
        recalcDate = False
        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentFDPctDF'})
        if cachedDF.count() > 0:
            self.opponentFDPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentFDPct3QtlDF'})
        if cachedDF.count() > 0:
            self.opponentFDPct3QtlDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentFDPct1QtlDF'})
        if cachedDF.count() > 0:
            self.opponentFDPct1QtlDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentDKPctDF'})
        if cachedDF.count() > 0:
            self.opponentDKPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentDKPct3QtlDF'})
        if cachedDF.count() > 0:
            self.opponentDKPct3QtlDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'opponentDKPct1QtlDF'})
        if cachedDF.count() > 0:
            self.opponentDKPct1QtlDF = pd.DataFrame(list(cachedDF))
        else:
            recalcDate = True

        recalcHomeAway = False
        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'homeAwayPctDF'})
        if cachedDF.count() > 0:
            self.homeAwayPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcHomeAway = True

        recalcRecent = False
        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'recentDiffPctDF'})
        if cachedDF.count() > 0:
            self.recentDiffPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcRecent = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'recent3QtlPctDF'})
        if cachedDF.count() > 0:
            self.recent3QtlPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcRecent = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'recent1QtlPctDF'})
        if cachedDF.count() > 0:
            self.recent1QtlPctDF = pd.DataFrame(list(cachedDF))
        else:
            recalcRecent = True

        if recalcRecent:
            self.GetRecentPlayerPercent(currentDF)

        getPlayerHistory = False
        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'matchupDF'})
        if cachedDF.count() > 0:
            self.matchupDF = pd.DataFrame(list(cachedDF))
        else:
            getPlayerHistory = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'matchup3QtlDF'})
        if cachedDF.count() > 0:
            self.matchup3QtlDF = pd.DataFrame(list(cachedDF))
        else:
            getPlayerHistory = True

        cachedDF = self.db.nbaBacktestData.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2], 'type': 'matchup1QtlDF'})
        if cachedDF.count() > 0:
            self.matchup1QtlDF = pd.DataFrame(list(cachedDF))
        else:
            getPlayerHistory = True

        if recalcDate:
            if self.currentDate[1] > 9:
                gamesDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
                {
                    "$match" : {
                        "$or" : [
                                {"year": {"$eq" : self.currentDate[0]},
                                 "month" : {"$gt" : 9, "$lt": self.currentDate[1]}},
                                {"year" : {"$eq" : self.currentDate[0]},
                                 "month" : {"$eq" : self.currentDate[1]},
                                 "date" : {"$lt" : self.currentDate[2]}}
                        ]
                    }
                }
                ])))
            else:
                gamesDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
                {
                    "$match" : {
                        "$or" : [
                                {"year": {"$eq" : self.currentDate[0]},
                                 "month" : {"$lt" : self.currentDate[1]}},
                                 {"year" : {"$eq" : self.currentDate[0]},
                                 "month" : {"$eq" : self.currentDate[1]},
                                 "date"  : {"$lt" : self.currentDate[2]}},
                                {"year" : {"$eq" : self.currentDate[0]-1},
                                 "month" : {"$gt" : 9}}
                        ]
                    }
                }
                ])))

            gamesDF = self.GetDiffPct(gamesDF)
            cols = gamesDF.select_dtypes(np.number).columns
            gamesDF[cols] = gamesDF[cols].clip(0.1)
            self.opponentFDPctDF = gamesDF.groupby(['fdPosition', 'opp_id']).mean()
            self.opponentFDPctDF = self.opponentFDPctDF.reset_index()
            self.opponentFDPct3QtlDF = gamesDF.groupby(['fdPosition', 'opp_id']).quantile([0.8])
            self.opponentFDPct3QtlDF = self.opponentFDPct3QtlDF.reset_index()
            self.opponentFDPct1QtlDF = gamesDF.groupby(['fdPosition', 'opp_id']).quantile([0.25])
            self.opponentFDPct1QtlDF = self.opponentFDPct1QtlDF.reset_index()

            self.opponentDKPctDF = gamesDF.groupby(['dkPosition', 'opp_id']).mean()
            self.opponentDKPctDF = self.opponentDKPctDF.reset_index()
            self.opponentDKPct3QtlDF = gamesDF.groupby(['dkPosition', 'opp_id']).quantile([0.8])
            self.opponentDKPct3QtlDF = self.opponentDKPct3QtlDF.reset_index()
            self.opponentDKPct1QtlDF = gamesDF.groupby(['dkPosition', 'opp_id']).quantile([0.25])
            self.opponentDKPct1QtlDF = self.opponentDKPct1QtlDF.reset_index()

            self.opponentFDPctDF['year'] = self.currentDate[0]
            self.opponentFDPctDF['month'] = self.currentDate[1]
            self.opponentFDPctDF['date'] = self.currentDate[2]
            self.opponentFDPctDF['type'] = 'opponentFDPctDF'
            self.opponentFDPct3QtlDF['year'] = self.currentDate[0]
            self.opponentFDPct3QtlDF['month'] = self.currentDate[1]
            self.opponentFDPct3QtlDF['date'] = self.currentDate[2]
            self.opponentFDPct3QtlDF['type'] = 'opponentFDPct3QtlDF'
            self.opponentFDPct1QtlDF['year'] = self.currentDate[0]
            self.opponentFDPct1QtlDF['month'] = self.currentDate[1]
            self.opponentFDPct1QtlDF['date'] = self.currentDate[2]
            self.opponentFDPct1QtlDF['type'] = 'opponentFDPct1QtlDF'

            self.opponentDKPctDF['year'] = self.currentDate[0]
            self.opponentDKPctDF['month'] = self.currentDate[1]
            self.opponentDKPctDF['date'] = self.currentDate[2]
            self.opponentDKPctDF['type'] = 'opponentDKPctDF'
            self.opponentDKPct3QtlDF['year'] = self.currentDate[0]
            self.opponentDKPct3QtlDF['month'] = self.currentDate[1]
            self.opponentDKPct3QtlDF['date'] = self.currentDate[2]
            self.opponentDKPct3QtlDF['type'] = 'opponentDKPct3QtlDF'
            self.opponentDKPct1QtlDF['year'] = self.currentDate[0]
            self.opponentDKPct1QtlDF['month'] = self.currentDate[1]
            self.opponentDKPct1QtlDF['date'] = self.currentDate[2]
            self.opponentDKPct1QtlDF['type'] = 'opponentDKPct1QtlDF'

            self.db.nbaBacktestData.insert_many(self.opponentFDPctDF.to_dict('records'))
            self.db.nbaBacktestData.insert_many(self.opponentFDPct3QtlDF.to_dict('records'))
            self.db.nbaBacktestData.insert_many(self.opponentFDPct1QtlDF.to_dict('records'))

            self.db.nbaBacktestData.insert_many(self.opponentDKPctDF.to_dict('records'))
            self.db.nbaBacktestData.insert_many(self.opponentDKPct3QtlDF.to_dict('records'))
            self.db.nbaBacktestData.insert_many(self.opponentDKPct1QtlDF.to_dict('records'))

        if recalcHomeAway or getPlayerHistory:
            if self.currentDate[1] > 9:
                gamesPointDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
                {
                    "$match" : {
                        "$or" : [
                                {"year": {"$eq" : self.currentDate[0]},
                                 "month" : {"$gt" : 9, "$lt": self.currentDate[1]}},
                                {"year" : {"$eq" : self.currentDate[0]},
                                 "month" : {"$eq" : self.currentDate[1]},
                                 "date" : {"$lt" : self.currentDate[2]}}
                        ]
                    }
                }
                ])))
            else:
                gamesPointDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
                {
                    "$match" : {
                        "$or" : [
                                {"year": {"$eq" : self.currentDate[0]},
                                 "month" : {"$lt" : self.currentDate[1]}},
                                 {"year" : {"$eq" : self.currentDate[0]},
                                 "month" : {"$eq" : self.currentDate[1]},
                                 "date"  : {"$lt" : self.currentDate[2]}},
                                {"year" : {"$eq" : self.currentDate[0]-1},
                                 "month" : {"$gt" : 9}}
                        ]
                    }
                }
                ])))
            gamesPointDF = self.GetDiff(gamesPointDF)

            if recalcHomeAway:
                self.homeAwayPctDF = gamesPointDF.groupby(['name', 'homeAway']).mean()
                self.homeAwayPctDF = self.homeAwayPctDF.reset_index()

                self.homeAwayPctDF['year'] = self.currentDate[0]
                self.homeAwayPctDF['month'] = self.currentDate[1]
                self.homeAwayPctDF['date'] = self.currentDate[2]
                self.homeAwayPctDF['type'] = 'homeAwayPctDF'

                self.db.nbaBacktestData.insert_many(self.homeAwayPctDF.to_dict('records'))
            if getPlayerHistory:
                self.matchupDF = gamesPointDF.groupby(['name', 'opp_id']).mean()
                self.matchupDF = self.matchupDF.reset_index()
                self.matchup3QtlDF = gamesPointDF.groupby(['name', 'opp_id']).quantile([0.8])
                self.matchup3QtlDF = self.matchup3QtlDF.reset_index()
                self.matchup1QtlDF = gamesPointDF.groupby(['name', 'opp_id']).quantile([0.25])
                self.matchup1QtlDF = self.matchup1QtlDF.reset_index()

                self.matchupDF['year'] = self.currentDate[0]
                self.matchupDF['month'] = self.currentDate[1]
                self.matchupDF['date'] = self.currentDate[2]
                self.matchupDF['type'] = 'matchupDF'
                self.matchup3QtlDF['year'] = self.currentDate[0]
                self.matchup3QtlDF['month'] = self.currentDate[1]
                self.matchup3QtlDF['date'] = self.currentDate[2]
                self.matchup3QtlDF['type'] = 'matchup3QtlDF'
                self.matchup1QtlDF['year'] = self.currentDate[0]
                self.matchup1QtlDF['month'] = self.currentDate[1]
                self.matchup1QtlDF['date'] = self.currentDate[2]
                self.matchup1QtlDF['type'] = 'matchup1QtlDF'

                self.db.nbaBacktestData.insert_many(self.matchupDF.to_dict('records'))
                self.db.nbaBacktestData.insert_many(self.matchup3QtlDF.to_dict('records'))
                self.db.nbaBacktestData.insert_many(self.matchup1QtlDF.to_dict('records'))

    def GetDiffPct(self, df):
        df = df.replace({'name': tr.playerTranslation})
        df = df.set_index('name', drop=False)

        df['pts'] = df.apply(lambda row: float(row.pts), axis=1)
        df['fg2'] = df.apply(lambda row: float(row.fg) - float(row.fg3), axis=1)
        df['fg3'] = df.apply(lambda row: float(row.fg3), axis=1)
        df['fta'] = df.apply(lambda row: float(row.fta), axis=1)
        df['ast'] = df.apply(lambda row: float(row.ast), axis=1)
        df['orb'] = df.apply(lambda row: float(row.orb), axis=1)
        df['drb'] = df.apply(lambda row: float(row.drb), axis=1)
        df['stl'] = df.apply(lambda row: float(row.stl), axis=1)
        df['blk'] = df.apply(lambda row: float(row.blk), axis=1)
        df['tov'] = df.apply(lambda row: float(row.tov), axis=1)
        df['mp'] = df.apply(lambda row: float(row.mp), axis=1)

        df = df[df['name'].isin(self.playersDF['name'])]

        df['pts_diff_pct'] = df.apply(lambda row: ((row.pts / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['pts_per_min']), axis=1)
        df['fg2_diff_pct'] = df.apply(lambda row: ((row.fg2 / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fg2_per_min']), axis=1)
        df['fg3_diff_pct'] = df.apply(lambda row: ((row.fg3 / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fg3_per_min']), axis=1)
        df['fta_diff_pct'] = df.apply(lambda row: ((row.fta / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fta_per_min']), axis=1)
        df['orb_diff_pct'] = df.apply(lambda row: ((row.orb / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['orb_per_min']), axis=1)
        df['drb_diff_pct'] = df.apply(lambda row: ((row.drb / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['drb_per_min']), axis=1)
        df['ast_diff_pct'] = df.apply(lambda row: ((row.ast / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['ast_per_min']), axis=1)
        df['stl_diff_pct'] = df.apply(lambda row: ((row.stl / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['stl_per_min']), axis=1)
        df['blk_diff_pct'] = df.apply(lambda row: ((row.blk / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['blk_per_min']), axis=1)
        df['tov_diff_pct'] = df.apply(lambda row: ((row.tov / row.mp) if row.mp > 0 else 0) / (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['tov_per_min']), axis=1)

        return df.fillna(0)
    def GetDiff(self, df):
        df = df.replace({'name': tr.playerTranslation})
        df = df.set_index('name', drop=False)

        df['pts'] = df.apply(lambda row: float(row.pts), axis=1)
        df['fg2'] = df.apply(lambda row: float(row.fg) - float(row.fg3), axis=1)
        df['fg3'] = df.apply(lambda row: float(row.fg3), axis=1)
        df['fta'] = df.apply(lambda row: float(row.fta), axis=1)
        df['ast'] = df.apply(lambda row: float(row.ast), axis=1)
        df['orb'] = df.apply(lambda row: float(row.orb), axis=1)
        df['drb'] = df.apply(lambda row: float(row.drb), axis=1)
        df['stl'] = df.apply(lambda row: float(row.stl), axis=1)
        df['blk'] = df.apply(lambda row: float(row.blk), axis=1)
        df['tov'] = df.apply(lambda row: float(row.tov), axis=1)
        df['mp'] = df.apply(lambda row: float(row.mp), axis=1)

        df = df[df['name'].isin(self.playersDF['name'])]

        df['pts_diff_pct'] = df.apply(lambda row: ((row.pts / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['pts_per_min']), axis=1)
        df['fg2_diff_pct'] = df.apply(lambda row: ((row.fg2 / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fg2_per_min']), axis=1)
        df['fg3_diff_pct'] = df.apply(lambda row: ((row.fg3 / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fg3_per_min']), axis=1)
        df['fta_diff_pct'] = df.apply(lambda row: ((row.fta / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fta_per_min']), axis=1)
        df['orb_diff_pct'] = df.apply(lambda row: ((row.orb / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['orb_per_min']), axis=1)
        df['drb_diff_pct'] = df.apply(lambda row: ((row.drb / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['drb_per_min']), axis=1)
        df['ast_diff_pct'] = df.apply(lambda row: ((row.ast / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['ast_per_min']), axis=1)
        df['stl_diff_pct'] = df.apply(lambda row: ((row.stl / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['stl_per_min']), axis=1)
        df['blk_diff_pct'] = df.apply(lambda row: ((row.blk / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['blk_per_min']), axis=1)
        df['tov_diff_pct'] = df.apply(lambda row: ((row.tov / row.mp) if row.mp > 0 else 0) - (self.playersDF[self.playersDF['name'] == row.name].iloc[0]['tov_per_min']), axis=1)

        return df.fillna(0)

    def GetRecentPlayerPercent(self, currentDF):
        # if self.currentDate[1] == 1:
        #     recentDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
        #     {
        #         "$match" : {
        #             "$or" : [
        #                     {"year": {"$eq" : self.currentDate[0] - 1},
        #                      "month" : {"$eq" : 12},
        #                      "date" : {"$gt" : self.currentDate[2]}},
        #                      {"year" : {"$eq" : self.currentDate[0]},
        #                      "month" : {"$eq" : self.currentDate[1]},
        #                      "date" : {"$lt" : self.currentDate[2]}}
        #             ]
        #         }
        #     }
        #     ])))
        # else:
        #     recentDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
        #     {
        #         "$match" : {
        #             "$or" : [
        #                     {"year": {"$eq" : self.currentDate[0]},
        #                      "month" : {"$eq" : self.currentDate[1] - 1},
        #                      "date" : {"$gt" : self.currentDate[2] if MONTH_TO_DATE[self.currentDate[1]-1] > self.currentDate[2] else MONTH_TO_DATE[self.currentDate[1]-1]}},
        #                      {"year" : {"$eq" : self.currentDate[0]},
        #                      "month" : {"$eq" : self.currentDate[1]},
        #                      "date" : {"$lt" : self.currentDate[2]}}
        #             ]
        #         }
        #     }
        #     ])))
        if self.currentDate[2] >= 15:
            recentDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
            {
                "$match" : {"year" : {"$eq" : self.currentDate[0]},
                             "month" : {"$eq" : self.currentDate[1]},
                             "date" : {"$gt" : self.currentDate[2] - 14, "$lt" : self.currentDate[2]}}
            }
            ])))
        elif self.currentDate[1] == 1:
            recentDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
            {
                "$match" : {
                    "$or" : [
                            {"year": {"$eq" : self.currentDate[0] - 1},
                             "month" : {"$eq" : 12},
                             "date" : {"$gt" : self.currentDate[2] + MONTH_TO_DATE[12] - 14}},
                             {"year" : {"$eq" : self.currentDate[0]},
                             "month" : {"$eq" : self.currentDate[1]},
                             "date" : {"$lt" : self.currentDate[2]}}
                    ]
                }
            }
            ])))
        else:
            recentDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
            {
                "$match" : {
                    "$or" : [
                            {"year": {"$eq" : self.currentDate[0]},
                             "month" : {"$eq" : self.currentDate[1] - 1},
                             "date" : {"$gt" : self.currentDate[2] + MONTH_TO_DATE[self.currentDate[1]-1] - 14}},
                             {"year" : {"$eq" : self.currentDate[0]},
                             "month" : {"$eq" : self.currentDate[1]},
                             "date" : {"$lt" : self.currentDate[2]}}
                    ]
                }
            }
            ])))

        recentDF = recentDF.replace({'name': tr.playerTranslation})
        recentDF = recentDF[recentDF['name'].isin(currentDF['name'])]

        recentDF = self.GetDiff(recentDF)
        self.recentDiffPctDF = recentDF[recentDF['blk_diff_pct'] < 100].groupby(['name']).mean()
        self.recentDiffPctDF = self.recentDiffPctDF.reset_index()
        self.recent3QtlPctDF = recentDF[recentDF['blk_diff_pct'] < 100].groupby(['name']).quantile([0.8])
        self.recent3QtlPctDF = self.recent3QtlPctDF.reset_index()
        self.recent1QtlPctDF = recentDF[recentDF['blk_diff_pct'] < 100].groupby(['name']).quantile([0.25])
        self.recent1QtlPctDF = self.recent1QtlPctDF.reset_index()

        self.recentDiffPctDF['year'] = self.currentDate[0]
        self.recentDiffPctDF['month'] = self.currentDate[1]
        self.recentDiffPctDF['date'] = self.currentDate[2]
        self.recentDiffPctDF['type'] = 'recentDiffPctDF'

        self.recent3QtlPctDF['year'] = self.currentDate[0]
        self.recent3QtlPctDF['month'] = self.currentDate[1]
        self.recent3QtlPctDF['date'] = self.currentDate[2]
        self.recent3QtlPctDF['type'] = 'recent3QtlPctDF'

        self.recent1QtlPctDF['year'] = self.currentDate[0]
        self.recent1QtlPctDF['month'] = self.currentDate[1]
        self.recent1QtlPctDF['date'] = self.currentDate[2]
        self.recent1QtlPctDF['type'] = 'recent1QtlPctDF'

        self.db.nbaBacktestData.insert_many(self.recentDiffPctDF.to_dict('records'))
        self.db.nbaBacktestData.insert_many(self.recent3QtlPctDF.to_dict('records'))
        self.db.nbaBacktestData.insert_many(self.recent1QtlPctDF.to_dict('records'))

    def GetPlayerOpponentHistory(self, currentDF):
        gamesDF = pd.DataFrame(list(self.db.nbaGameLogs.aggregate([
        {
            "$match" : {
                "$or" : [
                        {"year"  : {"$lt" : self.currentDate[0]}},
                        {"year"  : {"$eq" : self.currentDate[0]},
                         "month" : {"$lt" : self.currentDate[1]}},
                        {"year"  : {"$eq" : self.currentDate[0]},
                         "month" : {"$eq" : self.currentDate[1]},
                         "date"  : {"$lt"  : self.currentDate[2]}}
                ]
            }
        }
        ])))
        gamesDF = gamesDF.fillna(0)
        gamesDF = gamesDF.replace({'name': tr.playerTranslation})

        gamesDF = gamesDF[gamesDF['name'].isin(currentDF['name'])]

        gamesDF = self.GetDiff(gamesDF)
        self.matchupDF = gamesDF.groupby(['name', 'opp_id']).mean()
        self.matchupDF = self.matchupDF.reset_index()
        self.matchup3QtlDF = gamesDF.groupby(['name', 'opp_id']).quantile([0.8])
        self.matchup3QtlDF = self.matchup3QtlDF.reset_index()
        self.matchup1QtlDF = gamesDF.groupby(['name', 'opp_id']).quantile([0.25])
        self.matchup1QtlDF = self.matchup1QtlDF.reset_index()

        self.matchupDF['year'] = self.currentDate[0]
        self.matchupDF['month'] = self.currentDate[1]
        self.matchupDF['date'] = self.currentDate[2]
        self.matchupDF['type'] = 'matchupDF'
        self.matchup3QtlDF['year'] = self.currentDate[0]
        self.matchup3QtlDF['month'] = self.currentDate[1]
        self.matchup3QtlDF['date'] = self.currentDate[2]
        self.matchup3QtlDF['type'] = 'matchup3QtlDF'
        self.matchup1QtlDF['year'] = self.currentDate[0]
        self.matchup1QtlDF['month'] = self.currentDate[1]
        self.matchup1QtlDF['date'] = self.currentDate[2]
        self.matchup1QtlDF['type'] = 'matchup1QtlDF'

        self.db.nbaBacktestData.insert_many(self.matchupDF.to_dict('records'))
        self.db.nbaBacktestData.insert_many(self.matchup3QtlDF.to_dict('records'))
        self.db.nbaBacktestData.insert_many(self.matchup1QtlDF.to_dict('records'))

    def FindRecentRest(self):
        if self.currentDate[2] == 1:
            if self.currentDate[1] == 1:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]-2})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]-1})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]})
            else:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]-2})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]-1})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]})
        elif self.currentDate[2] == 2:
            if self.currentDate[1] == 1:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]-1})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': 1, 'date': 1})
            else:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]-1})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': 1})
        elif self.currentDate[2] == 3:
            if self.currentDate[1] == 1:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0] - 1, 'month': 12, 'date': MONTH_TO_DATE[12]})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': 1, 'date': 1})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': 1, 'date': 2})
            else:
                threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1]-1, 'date': MONTH_TO_DATE[self.currentDate[1]-1]})
                twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': 1})
                previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': 2})
        else:
            threeDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2]-3})
            twoDaysPreviousResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2]-2})
            previousDayResult = self.db.nbaGameLogs.find({'year': self.currentDate[0], 'month': self.currentDate[1], 'date': self.currentDate[2]-1})

        for player in threeDaysPreviousResult:
            self.threeDaysPrevious.append(tr.playerTranslation[player['name']] if player['name'] in tr.playerTranslation else player['name'])
        for player in twoDaysPreviousResult:
            self.twoDaysPrevious.append(tr.playerTranslation[player['name']] if player['name'] in tr.playerTranslation else player['name'])
        for player in previousDayResult:
            self.oneDayPrevious.append(tr.playerTranslation[player['name']] if player['name'] in tr.playerTranslation else player['name'])

    def AnalyzeCurrentSlate(self, currentDayDF):
        currentDayDF = currentDayDF[currentDayDF['name'].isin(self.playersDF['name'])]
        currentDayDF = currentDayDF[currentDayDF['name'].isin(self.homeAwayPctDF['name'])]
        # currentDayDF = currentDayDF[currentDayDF['name'].isin(self.recentDiffPctDF['name'])]
        currentDayDF['played1back'] = currentDayDF.apply(lambda row: row.name in self.oneDayPrevious, axis=1)
        currentDayDF['played2back'] = currentDayDF.apply(lambda row: row.name in self.twoDaysPrevious, axis=1)
        currentDayDF['played3back'] = currentDayDF.apply(lambda row: row.name in self.threeDaysPrevious, axis=1)

        if self.isBackTesting:
            currentDayDF['actualFDPoints'] = currentDayDF.apply(lambda row: float(row.pts) + 1.2*float(row.trb) + 1.5*float(row.ast) + 3*float(row.stl) + 3*float(row.blk) - float(row.tov), axis=1)
            currentDayDF['actualDKPoints'] = currentDayDF.apply(lambda row: float(row.pts) + 1.25*float(row.trb) + 1.5*float(row.ast) + 2*float(row.stl) + 2*float(row.blk) - 0.5*float(row.tov) + 0.5*float(row.fg3), axis=1)
        else:
            currentDayDF['actualFDPoints'] = 0
            currentDayDF['actualDKPoints'] = 0

        # self.opponentFDPctDF.to_csv('OpponentDefence.csv')
        optimalTeams, optimalDKTeams, expectedPointsDF = self.GenerateProjections(self.opponentFDPctDF, self.opponentDKPctDF, self.recentDiffPctDF, currentDayDF, self.homeAwayPctDF, self.matchupDF)
        # expectedPointsDF.to_csv('AveragePoints.csv')
        # self.matchupDF.to_csv('Matchups.csv')
        # self.opponentFDPctDF.to_csv('FDdefence.csv')
        # self.opponentDKPctDF.to_csv('DKdefence.csv')
        # self.homeAwayDF.to_csv('HomeAway.csv')

        optimalOptimisticTeams, optimalOptimsticDKTeams, expectedPointsDF = self.GenerateProjections(self.opponentFDPct3QtlDF, self.opponentDKPct3QtlDF, self.recent3QtlPctDF, currentDayDF, self.homeAwayPctDF, self.matchup3QtlDF)
        expectedPointsDF.to_csv('OptimisticPoints.csv')

        optimalPesimisticTeams, optimalPessimisticDKTeams, expectedPointsDF = self.GenerateProjections(self.opponentFDPct1QtlDF, self.opponentDKPct1QtlDF, self.recent1QtlPctDF, currentDayDF, self.homeAwayPctDF, self.matchup1QtlDF)
        expectedPointsDF.to_csv('PessimisticPoints.csv')

        # farOffDF = expectedPointsDF[expectedPointsDF['actualFDPoints'] > 0]

        # farOffDF['expectedVsReal'] = farOffDF.apply(lambda row: row.FDPoints / row.actualFDPoints, axis=1)
        # farOffDF['year'] = self.currentDate[0]
        # farOffDF['month'] = self.currentDate[1]
        # farOffDF['date'] = self.currentDate[2]

        # farOffDF = farOffDF[(farOffDF['FDPoints'] >= 12) | (farOffDF['actualFDPoints'] >= 12)]
        # with open('FarOffDifferences.csv', 'a') as f:
        #     farOffDF.to_csv(f, header=False)

        for team in optimalTeams:
            optimalTeam = team
        for team in optimalOptimisticTeams:
            optimalOptimisticTeam = team
        for team in optimalPesimisticTeams:
            optimalPesimisticTeam = team

        for team in optimalDKTeams:
            optimalDKTeam = team
        for team in optimalOptimsticDKTeams:
            optimalOptimisticDKTeam = team
        for team in optimalPessimisticDKTeams:
            optimalPesimisticDKTeam = team

        return (optimalTeam, optimalOptimisticTeam, optimalPesimisticTeam, optimalDKTeam, optimalOptimisticDKTeam, optimalPesimisticDKTeam)

    def GenerateProjections(self, averageDF, averageDKDF, recentDF, currentDF, homeAwayDF, matchupDF):
        # homeAwayDF['pts_diff_pct'] = homeAwayDF.apply(lambda row: row.pts_diff_pct ** (1./2), axis=1)
        # homeAwayDF['fg2_diff_pct'] = homeAwayDF.apply(lambda row: row.fg2_diff_pct ** (1./2), axis=1)
        # homeAwayDF['fg3_diff_pct'] = homeAwayDF.apply(lambda row: row.fg3_diff_pct ** (1./2), axis=1)
        # homeAwayDF['fta_diff_pct'] = homeAwayDF.apply(lambda row: row.fta_diff_pct ** (1./2), axis=1)
        # homeAwayDF['orb_diff_pct'] = homeAwayDF.apply(lambda row: row.orb_diff_pct ** (1./2), axis=1)
        # homeAwayDF['drb_diff_pct'] = homeAwayDF.apply(lambda row: row.drb_diff_pct ** (1./2), axis=1)
        # homeAwayDF['ast_diff_pct'] = homeAwayDF.apply(lambda row: row.ast_diff_pct ** (1./2), axis=1)
        # homeAwayDF['stl_diff_pct'] = homeAwayDF.apply(lambda row: row.stl_diff_pct ** (1./2), axis=1)
        # homeAwayDF['blk_diff_pct'] = homeAwayDF.apply(lambda row: row.blk_diff_pct ** (1./2), axis=1)
        # homeAwayDF['tov_diff_pct'] = homeAwayDF.apply(lambda row: row.tov_diff_pct ** (1./2), axis=1)

        matchupDF['pts_diff_pct'] = matchupDF.apply(lambda row: row.pts_diff_pct / 4, axis=1)
        matchupDF['fg2_diff_pct'] = matchupDF.apply(lambda row: row.fg2_diff_pct / 4, axis=1)
        matchupDF['fg3_diff_pct'] = matchupDF.apply(lambda row: row.fg3_diff_pct / 4, axis=1)
        matchupDF['fta_diff_pct'] = matchupDF.apply(lambda row: row.fta_diff_pct / 4, axis=1)
        matchupDF['orb_diff_pct'] = matchupDF.apply(lambda row: row.orb_diff_pct / 4, axis=1)
        matchupDF['drb_diff_pct'] = matchupDF.apply(lambda row: row.drb_diff_pct / 4, axis=1)
        matchupDF['ast_diff_pct'] = matchupDF.apply(lambda row: row.ast_diff_pct / 4, axis=1)
        matchupDF['stl_diff_pct'] = matchupDF.apply(lambda row: row.stl_diff_pct / 4, axis=1)
        matchupDF['blk_diff_pct'] = matchupDF.apply(lambda row: row.blk_diff_pct / 4, axis=1)
        matchupDF['tov_diff_pct'] = matchupDF.apply(lambda row: row.tov_diff_pct / 4, axis=1)

        overallOppDefDF = averageDF.groupby(['opp_id']).mean()
        overallOppDefDF['pts_diff_pct'] = overallOppDefDF.apply(lambda row: row.pts_diff_pct / 2, axis=1)
        overallOppDefDF['fg2_diff_pct'] = overallOppDefDF.apply(lambda row: row.fg2_diff_pct / 2, axis=1)
        overallOppDefDF['fg3_diff_pct'] = overallOppDefDF.apply(lambda row: row.fg3_diff_pct / 2, axis=1)
        overallOppDefDF['fta_diff_pct'] = overallOppDefDF.apply(lambda row: row.fta_diff_pct / 2, axis=1)
        overallOppDefDF['orb_diff_pct'] = overallOppDefDF.apply(lambda row: row.orb_diff_pct / 2, axis=1)
        overallOppDefDF['drb_diff_pct'] = overallOppDefDF.apply(lambda row: row.drb_diff_pct / 2, axis=1)
        overallOppDefDF['ast_diff_pct'] = overallOppDefDF.apply(lambda row: row.ast_diff_pct / 2, axis=1)
        overallOppDefDF['stl_diff_pct'] = overallOppDefDF.apply(lambda row: row.stl_diff_pct / 2, axis=1)
        overallOppDefDF['blk_diff_pct'] = overallOppDefDF.apply(lambda row: row.blk_diff_pct / 2, axis=1)
        overallOppDefDF['tov_diff_pct'] = overallOppDefDF.apply(lambda row: row.tov_diff_pct / 2, axis=1)

        # recentDF['pts_diff_pct'] = recentDF.apply(lambda row: row.pts_diff_pct / 2, axis=1)
        # recentDF['fg2_diff_pct'] = recentDF.apply(lambda row: row.fg2_diff_pct / 2, axis=1)
        # recentDF['fg3_diff_pct'] = recentDF.apply(lambda row: row.fg3_diff_pct / 2, axis=1)
        # recentDF['fta_diff_pct'] = recentDF.apply(lambda row: row.fta_diff_pct / 2, axis=1)
        # recentDF['orb_diff_pct'] = recentDF.apply(lambda row: row.orb_diff_pct / 2, axis=1)
        # recentDF['drb_diff_pct'] = recentDF.apply(lambda row: row.drb_diff_pct / 2, axis=1)
        # recentDF['ast_diff_pct'] = recentDF.apply(lambda row: row.ast_diff_pct / 2, axis=1)
        # recentDF['stl_diff_pct'] = recentDF.apply(lambda row: row.stl_diff_pct / 2, axis=1)
        # recentDF['blk_diff_pct'] = recentDF.apply(lambda row: row.blk_diff_pct / 2, axis=1)
        # recentDF['tov_diff_pct'] = recentDF.apply(lambda row: row.tov_diff_pct / 2, axis=1)

        homeAwayDF = homeAwayDF.set_index(['name', 'homeAway'], drop=False)
        averageDF = averageDF.set_index(['fdPosition', 'opp_id'], drop=False)
        averageDKDF = averageDKDF.set_index(['dkPosition', 'opp_id'], drop=False)
        # matchupDF = matchupDF.set_index(['name', 'opp_id'], drop=False)
        recentDF = recentDF.set_index('name', drop=False)
        usageDiff = {}
        minAdd = {}
        for player in self.injuredPlayers:
            playerStats = self.db.nbaPlayers.find_one({'name': player,'season': '2019'})
            usage = playerStats['usg_pct']
            mp = (recentDF[recentDF['name'] == player]['mp']).iloc[0]
            playersOnTeam = self.db.nbaPlayers.find({'season': '2019', 'team_id': playerStats['team_id']})
            totalPositionMinutes = mp
            positionPlayer = {}
            for teammate in playersOnTeam:
                if teammate['name'] == player:
                    continue
                teammateName = tr.playerTranslation[teammate['name']] if teammate['name'] in tr.playerTranslation else teammate['name']

                teammateUsage = teammate['usg_pct'] if 'usg_pct' in teammate else 0
                usageDiff[teammateName] = usageDiff[teammateName] if teammateName in usageDiff else 0 + (float(teammateUsage) / 100 * float(usage)) / 20 + 1

                if teammate['pos'] == playerStats['pos']:
                    totalPositionMinutes += recentDF[recentDF['name'] == teammateName]['mp']
                    positionPlayer[teammate['name']] = recentDF.at[teammateName, 'mp'] if teammateName in recentDF.index else 0

            for key, value in positionPlayer.items():
                percentageOfMinutes = value / totalPositionMinutes
                expectedMinutesToAdd = percentageOfMinutes * mp
                if key in recentDF.index:
                    recentDF.at[key, 'mp'] = expectedMinutesToAdd + recentDF.at[key, 'mp']

        currentDayDF = currentDF.copy()
        # print(homeAwayDF[homeAwayDF['name'] == 'John Collins'])
        if self.isBackTesting:
            currentDayDF['mp_per_g'] = currentDayDF['mp']
        else:
            currentDayDF['mp_per_g'] = currentDayDF.apply(lambda row: self.playersDF.at[row.name, 'mp_per_g'], axis=1)

        for stat in self.importantStats:
            expectedStat = str.format('expected{0}', stat)
            expectedStatPerMin = str.format('expected{0}PerMin', stat)
            expectedDKStat = str.format('expectedDK{0}', stat)
            expectedDKStatPerMin = str.format('expectedDK{0}PerMin', stat)
            statPerMin = str.format('{0}_per_min', stat)
            statDiffPct = str.format('{0}_diff_pct', stat)

            currentDayDF[expectedStatPerMin] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, statPerMin])
                                                * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0][statDiffPct]
                                                + (recentDF.at[row.name, statDiffPct] if row.name in recentDF.index else 0)
                                                # + (overallOppDefDF.at[row.opp_id, 'pts_diff_pct'])
                                                + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0][statDiffPct] if ((homeAwayDF['name'] == row.name) & (homeAwayDF['homeAway'] == row.homeAway)).any() else 0)
                                                + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0][statDiffPct] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
                                                , axis=1)
            currentDayDF[expectedStat] = currentDayDF.apply(lambda row: row.mp_per_g * row[expectedStatPerMin], axis=1)

            currentDayDF[expectedDKStatPerMin] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, statPerMin])
                                                * (averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0][statDiffPct] if ((averageDKDF['dkPosition'] == row.dkPosition) & (averageDKDF['opp_id'] == row.opp_id)).any() else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0][statDiffPct])
                                                + (recentDF.at[row.name, statDiffPct] if row.name in recentDF.index else 0)
                                                # + (overallOppDefDF.at[row.opp_id, 'pts_diff_pct'])
                                                + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0][statDiffPct] if ((homeAwayDF['name'] == row.name) & (homeAwayDF['homeAway'] == row.homeAway)).any() else 0)
                                                + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0][statDiffPct] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
                                                , axis=1)
            currentDayDF[expectedDKStat] = currentDayDF.apply(lambda row: row.mp_per_g * row[expectedDKStatPerMin], axis=1)

        # currentDayDF['expectedDKPtsPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'pts_per_min'])
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['pts_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['pts_diff_pct']
        #                                     + (recentDF.at[row.name, 'pts_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'pts_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['pts_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['pts_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKPts'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKPtsPerMin, axis=1)

        # currentDayDF['expected3fgPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'fg3_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['fg3_diff_pct']
        #                                     + (recentDF.at[row.name, 'fg3_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, '3fg_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['fg3_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['fg3_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expected3fg'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expected3fgPerMin, axis=1)

        # currentDayDF['expectedDK3fgPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'fg3_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['fg3_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['fg3_diff_pct']
        #                                     + (recentDF.at[row.name, 'fg3_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, '3fg_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['fg3_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['fg3_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDK3fg'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDK3fgPerMin, axis=1)
        # # currentDayDF['expectedftaPerMin'] = currentDayDF.apply(lambda row: self.playersDF[self.playersDF['name'] == row.name].iloc[0]['fta_per_min'] *
        # #                                   averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['fta_diff_pct'] *
        # #                                   recentDF[recentDF['name'] == row.name].iloc[0]['fta_diff_pct'], axis=1)
        # # currentDayDF['expectedfta'] = currentDayDF.apply(lambda row: self.playersDF[self.playersDF['name'] == row.name].iloc[0]['mp_per_g'] * row.expectedftaPerMin, axis=1)

        # # currentDayDF['expectedPts'] = currentDayDF.apply(lambda row: row.expected3fg*3 + row.expected2fg*2 + row.expectedfta * self.playersDF[self.playersDF['name'] == row.name].iloc[0]['ft_pct'], axis=1)


        # currentDayDF['expectedOrbPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'orb_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['orb_diff_pct']
        #                                     + (recentDF.at[row.name, 'orb_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'orb_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['orb_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['orb_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedOrb'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedOrbPerMin, axis=1)

        # currentDayDF['expectedDKOrbPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'orb_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['orb_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['orb_diff_pct']
        #                                     + (recentDF.at[row.name, 'orb_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'orb_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['orb_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['orb_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKOrb'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKOrbPerMin, axis=1)

        # currentDayDF['expectedDrbPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'drb_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['drb_diff_pct']
        #                                     + (recentDF.at[row.name, 'drb_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'drb_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['drb_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['drb_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDrb'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDrbPerMin, axis=1)

        # currentDayDF['expectedDKDrbPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'drb_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['drb_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['drb_diff_pct']
        #                                     + (recentDF.at[row.name, 'drb_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'drb_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['drb_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['drb_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKDrb'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKDrbPerMin, axis=1)

        # currentDayDF['expectedAstPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'ast_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['ast_diff_pct']
        #                                     + (recentDF.at[row.name, 'ast_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'ast_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['ast_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['ast_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKAst'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedAstPerMin, axis=1)

        # currentDayDF['expectedDKAstPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'ast_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['ast_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['ast_diff_pct']
        #                                     + (recentDF.at[row.name, 'ast_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'ast_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['ast_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['ast_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedAst'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKAstPerMin, axis=1)

        # currentDayDF['expectedStlPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'stl_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['stl_diff_pct']
        #                                     + (recentDF.at[row.name, 'stl_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'stl_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['stl_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['stl_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedStl'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedStlPerMin, axis=1)

        # currentDayDF['expectedDKStlPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'stl_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['stl_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['stl_diff_pct']
        #                                     + (recentDF.at[row.name, 'stl_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'stl_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['stl_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['stl_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKStl'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKStlPerMin, axis=1)

        # currentDayDF['expectedBlkPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'blk_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['blk_diff_pct']
        #                                     + (recentDF.at[row.name, 'blk_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'blk_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['blk_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['blk_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedBlk'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedBlkPerMin, axis=1)

        # currentDayDF['expectedDKBlkPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'blk_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['blk_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['blk_diff_pct']
        #                                     + (recentDF.at[row.name, 'blk_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'blk_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['blk_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['blk_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKBlk'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKBlkPerMin, axis=1)

        # currentDayDF['expectedTovPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'tov_per_min'] )
        #                                     * averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['tov_diff_pct']
        #                                     + (recentDF.at[row.name, 'tov_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'tov_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['tov_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['tov_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedTov'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedTovPerMin, axis=1)

        # currentDayDF['expectedDKTovPerMin'] = currentDayDF.apply(lambda row: (self.playersDF.at[row.name, 'tov_per_min'] )
        #                                     * averageDKDF[averageDKDF['dkPosition'] == row.dkPosition][averageDKDF['opp_id'] == row.opp_id].iloc[0]['tov_diff_pct'] if (row.dkPosition, row.opp_id) in averageDKDF.index else averageDF[averageDF['fdPosition'] == row.fdPosition][averageDF['opp_id'] == row.opp_id].iloc[0]['tov_diff_pct']
        #                                     + (recentDF.at[row.name, 'tov_diff_pct'] if row.name in recentDF.index else 0)
        #                                     # + (overallOppDefDF.at[row.opp_id, 'tov_diff_pct'])
        #                                     # + (homeAwayDF[homeAwayDF['name'] == row.name][homeAwayDF['homeAway'] == row.homeAway].iloc[0]['tov_diff_pct'] if (row.name, row.homeAway) in homeAwayDF.index else 0)
        #                                     + (matchupDF[matchupDF['name'] == row.name][matchupDF['opp_id'] == row.opp_id].iloc[0]['tov_diff_pct'] if ((matchupDF['name'] == row.name) & (matchupDF['opp_id'] == row.opp_id)).any() else 0)
        #                                     , axis=1)
        # currentDayDF['expectedDKTov'] = currentDayDF.apply(lambda row: row.mp_per_g * row.expectedDKTovPerMin, axis=1)


        currentDayDF['restMultiplier'] = currentDayDF.apply(lambda row: NBADataAnalyzer.CalculateRestMultipliers(row.played1back, row.played2back, row.played3back), axis=1)

        # expectedPointsDF = currentDayDF[['name', 'fdPosition', 'salary', 'opp_id', 'expectedPts', 'expectedOrb', 'expectedDrb', 'expectedAst', 'expectedStl', 'expectedBlk','expectedTov', 'actualFDPoints', 'played1back', 'played2back', 'played3back']]

        currentDayDF['FDPointsPerMin'] = currentDayDF.apply(lambda row: (float(row.expectedDKptsPerMin) +
                                                                  1.2 * float(row.expectedDKdrbPerMin + row.expectedDKorbPerMin) +
                                                                  1.5 * float(row.expectedDKastPerMin) +
                                                                  3 * float(row.expectedDKblkPerMin) +
                                                                  3 * float(row.expectedDKstlPerMin) -
                                                                  float(row.expectedDKtovPerMin))
                                                                  # * row.restMultiplier
                                                                  , axis=1)

        currentDayDF['DKPointsPerMin'] = currentDayDF.apply(lambda row: (float(row.expectedDKptsPerMin) +
                                                                  1.25 * float(row.expectedDKdrbPerMin + row.expectedDKorbPerMin) +
                                                                  1.5 * float(row.expectedDKastPerMin) +
                                                                  2 * float(row.expectedDKblkPerMin) +
                                                                  2 * float(row.expectedDKstlPerMin) +
                                                                  0.5 * float(row.expectedDKfg3PerMin) -
                                                                  0.5 * float(row.expectedDKtovPerMin))
                                                                  # * row.restMultiplier
                                                                  , axis=1)

        currentDayDF['FDPoints'] = currentDayDF.apply(lambda row: (float(row.expectedpts) +
                                                                  1.2 * float(row.expecteddrb + row.expectedorb) +
                                                                  1.5 * float(row.expectedast) +
                                                                  3 * float(row.expectedblk) +
                                                                  3 * float(row.expectedstl) -
                                                                  float(row.expectedtov))
                                                                  # * row.restMultiplier
                                                                  * (usageDiff[row.name] if row.name in usageDiff else 1)
                                                                  , axis=1)

        currentDayDF['DKPoints'] = currentDayDF.apply(lambda row: (float(row.expectedDKptsPerMin) +
                                                                  1.25 * float(row.expectedDKdrbPerMin + row.expectedDKorbPerMin) +
                                                                  1.5 * float(row.expectedDKastPerMin) +
                                                                  2 * float(row.expectedDKblkPerMin) +
                                                                  2 * float(row.expectedDKstlPerMin) +
                                                                  0.5 * float(row.expectedDKfg3PerMin) -
                                                                  0.5 * float(row.expectedDKtovPerMin))
                                                                  # * row.restMultiplier
                                                                  * (usageDiff[row.name] if row.name in usageDiff else 1)
                                                                  , axis=1)

        currentDayDF['FDPoints'] = currentDayDF['FDPoints'].replace([np.inf, -np.inf, np.nan], 0)
        currentDayDF['DKPoints'] = currentDayDF['FDPoints'].replace([np.inf, -np.inf, np.nan], 0)

        playersList = [(Player(row.name, '', row.name, [row.fdPosition], row.opp_id, row.salary, row.FDPoints, row.actualFDPoints)) for index, row in currentDayDF.iterrows()]
        # expectedPointsDF = expectedPointsDF[expectedPointsDF['FDPoints'] > 10]
        currentPoints = 1000
        maxRealPoints = 0
        # print(playersList)

        optimizer = get_optimizer(Site.FANDUEL, Sport.BASKETBALL)
        optimizer.load_players(playersList)

        lineups = optimizer.optimize(n=1)

        playersList = [(Player(row.name, '', row.name, row.dkPosition.split('/'), row.opp_id, row.dkSalary, row.DKPoints, row.actualDKPoints)) for index, row in currentDayDF.iterrows()]

        optimizer = get_optimizer(Site.DRAFTKINGS, Sport.BASKETBALL)
        optimizer.load_players(playersList)

        dkLineups = optimizer.optimize(n=1)
        expectedPointsDF = currentDayDF[['fdPosition', 'dkPosition', 'salary', 'dkSalary', 'opp_id', 'expectedDKptsPerMin', 'expectedDKfg3PerMin', 'expectedDKorbPerMin', 'expectedDKdrbPerMin', 'expectedDKastPerMin', 'expectedDKstlPerMin', 'expectedDKblkPerMin','expectedDKtovPerMin', 'actualFDPoints', 'actualDKPoints', 'mp_per_g', 'restMultiplier', 'FDPointsPerMin', 'DKPointsPerMin']]

        return lineups, dkLineups, expectedPointsDF

    def CalculateRestMultipliers(oneDay, twoDay, threeDay):
        if oneDay and threeDay:
            return 0.8
        elif oneDay:
            return 0.97
        # elif twoDay and threeDay:
        #     return 0.95
        # elif not oneDay and not twoDay:
        #     return 1.2
        else:
            return 1
