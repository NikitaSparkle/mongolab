import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def check_and_create_collection(db, collection_name):
    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' already exists.")
    else:
        db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created.")

def load_json_to_mongodb(json_file, collection):
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)
    print("Data loaded successfully.")

uri = "mongodb+srv://NickPartas:NikMongo338199@cluster0.y9zohbq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client['labdb']
collection_name = 'movies'
check_and_create_collection(db, collection_name)
movies_collection = db[collection_name]

# Використовуй повний шлях до файлу, якщо потрібно
json_file_path = 'movies.json'
load_json_to_mongodb(json_file_path, movies_collection)

print("Databases", client.list_database_names())
print("Collections:", db.list_collection_names())

for movie in movies_collection.find():
    print(movie)
