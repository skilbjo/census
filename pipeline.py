#/usr/bin/env python3

import json
import petl as etl
import quandl
import configparser
config = configparser.ConfigParser()
config.read('./lib/config/config')

data_sets = {
	'FRED': ['GDP'],
	'IRS': ['RETURNS_IND_CUR']
}

def get_data(data_set):
	filename = data_set.lower().replace('/','_')
	write_file('./json/'+filename+'.json', api_request(data_set) )

def api_request(data_set):
	import quandl as q
	api_token = config.get('quandl','api_key')
	return q.get(data_set+'.json',authtoken = api_token).to_json()

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
	request = 'FRED'
	for data_set in data_sets[request]:
		# print(request+'/'+data_set)
		data = get_data(request+'/'+data_set)
	# 	data = read_data(data_set)
	# 	data = transform(data,data_set)
	# 	load(data,data_set)

if __name__ == '__main__':
	main()

