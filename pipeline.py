#/usr/bin/env python3

import json
import petl as etl
import pandas as pd
import configparser
config = configparser.ConfigParser()
config.read('./lib/config/config')

data_sets = {
	'FRED': ['GDP'],
	'IRS': ['RETURNS_IND_CUR']
}

def get_data(data_set):
	filename = data_set.lower().replace('/','_')
	return write_file('./json/'+filename+'.json', api_request(data_set) )

def api_request(data_set):
	import quandl as q
	api_token = config.get('quandl','api_key')
	data = q.get(data_set,authtoken = api_token).to_json(date_format='iso')
	# print(data)
	return data

def write_file(filename,data):
	with open(filename,'w') as f:
		return f.write(data)

def read_data(data_set):
	data = read_file('./json/'+data_set.lower()+'.json')
	return data

def read_file(filename):
	with open(filename,'r') as f:
		return json.load(f)

def transform_to_pandas(data):
	data = pd.DataFrame(data)
	data.reset_index(level=0, inplace=True)	
	return data

def transform_to_petl(data):
	isodate = etl.dateparser('%Y-%m-%d')
	data = etl.fromdataframe(data)
	data = etl.rename(data,{'index':'Date','VALUE':'Value'})
	data = etl.convert(data,{'Date': lambda d: d[:10]})
	data = etl.convert(data,{'Date': lambda d: isodate(d) })
	return data

def transform(data):
	data = transform_to_pandas(data)
	data = transform_to_petl(data)
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
	# etl.appenddb(data,conn,data_set.lower())
	etl.appenddb(data,conn,data_set.lower())	
	conn.close()
	print('Table {0} loaded.'.format(data_set))
	return

def main():
	request = 'FRED'
	for data_set in data_sets[request]:
		get_data(request+'/'+data_set)
		data = read_data(request+'_'+data_set)
		data = transform(data)
		print(data)
		load(data,data_set)

if __name__ == '__main__':
	main()

