import json
from os import listdir
from os.path import isfile, join


filePaths = [".\\data_from_willhaben.json"]

exportData = []

properties = [
	"coordinates", 
	"postcode", 
	"id",
	"orgname",

	"floor",
	"number_of_rooms",
	"location_quality",
	"estate_size",
	"estate_size/living_area", 
	"rooms",

	"price",

	"published",
]

for filePath in filePaths:
	print(filePath)
	with open(filePath) as file:
		data = json.load(file)
		for advert in data:
			objData = {}
			for attribute in advert:
				if attribute in properties:
					objData[attribute] = advert[attribute]

			if 'coordinates' not in objData:
				continue

			latLon = objData['coordinates'].split(",")

			objData['lat'] = latLon[0]
			objData['lon'] = latLon[1]
			del objData['coordinates']
		
			exportData.append(objData)
			
with open('result_for_db.json', 'w') as file:
	json.dump(exportData, file)
