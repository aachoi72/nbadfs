from pydfs_lineup_optimizer import get_optimizer, Site, Sport, Player, CSVLineupExporter
import sys
import pandas as pd

def RunOptimizer(playersList, numberOfLineups, site):
	optimizer = get_optimizer(site, Sport.BASKETBALL)
	optimizer.load_players(playersList)

	lineups = optimizer.optimize(n=numberOfLineups)

	return lineups

if __name__ == '__main__':
	nameOfFile = sys.argv[1]
	numberOfLineups = int(sys.argv[2])
	site = Site.FANDUEL if sys.argv[3] == 'F' else Site.DRAFTKINGS
	currentDayDF = pd.read_csv(nameOfFile)

	if site == Site.FANDUEL:
		playersList = [(Player(row.playerName, '', row.playerName, [row.fdPosition], row.opp_id, row.salary, row.FDPoints, 0)) for index, row in currentDayDF.iterrows()]

		lineups = RunOptimizer(playersList, numberOfLineups, site)

		for lineup in lineups:
			print(lineup)
	else:
		playersList = [(Player(row.playerName, '', row.playerName, row.dkPosition.split('/'), row.opp_id, row.dkSalary, row.DKPoints, 0)) for index, row in currentDayDF.iterrows()]

		lineups = RunOptimizer(playersList, numberOfLineups, site)

		for lineup in lineups:
			print(lineup)
