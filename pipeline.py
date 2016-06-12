#/usr/bin/env python3

import json
import petl as etl
import configparser
config = configparser.ConfigParser()
config.read('./lib/config/config')

api = {
	'base_url': 'https://api.census.gov/data',
	'series_prefix': '?series_id=',
	'series': {
		'Economic_Key_Stats': 'ewks',
		'International_Trade': 'inittrade'
	},
	'api_key': '&api_key='+config.get('census.gov','api_key')
}

years = [year for year in range(2000,2010 +1)]

def get_data(data_set):
	write_file('./json/'+data_set.lower()+'.json', api_request(data_set) )

def api_request(data_set):
	import requests as r
	api_url = (api['base_url']+api['series_prefix']+api['series'][data_set]
					+api['api_key']+api['file_type'])
	return r.get(api_url).json()

def write_file(filename,data):
	with open(filename,'w') as f:
		return json.dump(data,f, sort_keys=True, indent=4)

def read_data(data_set):
	data = read_file('./json/'+data_set.lower()+'.json')
	return data

def read_file(filename):
	with open(filename,'r') as f:
		return json.load(f)

def transform(data,data_set):
	data = data['observations']
	data = etl.fromdicts(data, header=['value','realtime_start','realtime_end','date'])
	data = etl.cut(data,'date','value')	
	data = etl.rename(data,{'date':'date','value': data_set.lower()})
	data = etl.convert(data,data_set.lower(),lambda val: 0 if val == '.' else val)
	return data

def load(data,data_set):
	import psycopg2

	conn = psycopg2.connect(	
		host 			= config.get('rpi','server'), 
		database	= config.get('rpi','database'), 
		user			=	config.get('rpi','user'), 
		password	= config.get('rpi','passwd')
	)
	conn.autocommit = True
	etl.appenddb(data,conn,data_set.lower())
	conn.close()
	print('Table {0} loaded.'.format(data_set))
	return

def main():
	print(years)
	# for data_set in series:
	# 	get_data(data_set)
	# 	data = read_data(data_set)
	# 	data = transform(data,data_set)
	# 	load(data,data_set)

if __name__ == '__main__':
	main()

