import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))
class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table_teams = my_DB.search('teams')
my_table_players = my_DB.search('players')
my_table_titanic = my_DB.search('titanic')
my_table3_filtered = my_table_players.filter(lambda x: 'ia' in x['team']).filter(lambda x: int(x['minutes']) < 200).filter(lambda x: int(x['passes']) > 100).select(['surname', 'team', 'position'])
print(f"{my_table3_filtered}\n")
below_10 = my_table_teams.filter(lambda x: int(x['ranking']) < 10)
above_10 = my_table_teams.filter(lambda x: int(x['ranking']) >= 10)
print(f"Team with ranking below 10  vs  Team with ranking above 10")
print(f"{below_10.aggregate(lambda x: sum(x)/len(x), 'games'):^25.3f}{above_10.aggregate(lambda x: sum(x)/len(x), 'games'):^35.3f}\n")
forwards = my_table_players.filter(lambda x: x['position'] == 'forward')
midfielders = my_table_players.filter(lambda x: x['position'] == 'midfielder')
print(f"forwards  vs  midfielders")
print(f"{forwards.aggregate(lambda x: sum(x)/len(x), 'passes'):^10.3f}{midfielders.aggregate(lambda x: sum(x)/len(x), 'passes'):^18.3f}\n")
class_1 = my_table_titanic.filter(lambda x: int(x['class']) == 1)
class_3 = my_table_titanic.filter(lambda x: int(x['class']) == 3)
print('Class 1   vs   Class 3')
print(f"{class_1.aggregate(lambda x: sum(x)/len(x), 'fare'):^10.3f}{class_3.aggregate(lambda x: sum(x)/len(x), 'fare'):^18.3f}\n")
male = my_table_titanic.filter(lambda x: x['gender'] == 'M')
survived_male = male.filter(lambda x: x['survived'] == 'yes')
female = my_table_titanic.filter(lambda x: x['gender'] == 'F')
survived_female = female.filter(lambda x: x['survived'] == 'yes')
print('survival rate of male   vs   survival rate of female')
print(f"{((len(survived_male.table)/len(male.table))*100):^20.3f}{((len(survived_female.table)/len(female.table))*100):^35.3f}")