import boto3
import time
import json
import pandas as pd

DeliveryStreamName = 'StreamMovies'
firehose = boto3.client('firehose')

record = {}
df = pd.read_csv('movie-netflix.csv', sep=';', encoding = 'UTF-8')

for index, row in df.iterrows():
	record = {
	'id': row['id'],
	'fecha': row['fecha'],
	'nombre' :  row['nombre']
	}
	response = firehose.put_record(
		DeliveryStreamName = DeliveryStreamName,
		Record = {
			'Data': json.dumps(record)
		}
	)
	print('Recomendacion de pelicula enviada a Kinesis Data Firehose : \n' + str(record))
	time.sleep(.5)