"""MongoDB load with batch inserts and indexes. Requires pymongo and a running MongoDB."""
import argparse,json
from pathlib import Path
from pymongo import MongoClient
p=argparse.ArgumentParser(); p.add_argument('--data',required=True); p.add_argument('--uri',default='mongodb://localhost:27017'); p.add_argument('--database',default='ecommerce_analytics'); a=p.parse_args()
db=MongoClient(a.uri,serverSelectionTimeoutMS=5000)[a.database]
for name,key in [('users','user_id'),('products','product_id'),('categories','category_id'),('transactions','transaction_id')]:
 path=Path(a.data)/f'{name}.json'
 if not path.exists(): raise FileNotFoundError(path)
 docs=json.loads(path.read_text(encoding='utf-8'))
 db[name].delete_many({})  # Idempotent reload; only execute against the intended project database.
 for i in range(0,len(docs),1000): db[name].insert_many(docs[i:i+1000],ordered=False)
 db[name].create_index(key,unique=True)
 print(name,db[name].count_documents({}))
db.transactions.create_index([('session_id',1)]); db.transactions.create_index([('user_id',1),('timestamp',-1)])
db.products.create_index('category_id')
