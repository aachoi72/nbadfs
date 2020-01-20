from pymongo import MongoClient
import sys

client = MongoClient()
db = client.test

player = sys.argv[1]
position = sys.argv[2]

db.nbaGameLogs.update_one(
{'name': player},
{
	'$set': {'actualPosition': position}
})