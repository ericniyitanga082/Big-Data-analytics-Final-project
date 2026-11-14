"""Create six publication-ready charts from actual Spark CSV outputs."""
import argparse
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=argparse.ArgumentParser(); p.add_argument('--out',default='outputs'); a=p.parse_args()
out=Path(a.out)
def read(name):
 path=out/name
 if not path.exists(): raise FileNotFoundError(f'Run Spark analytics first: {path}')
 return pd.read_csv(path)
def save(name):
 plt.tight_layout(); plt.savefig(out/name,dpi=180,bbox_inches='tight'); plt.close()
d=read('daily_revenue.csv'); d['date']=pd.to_datetime(d['date']); plt.figure(figsize=(10,4)); plt.plot(d['date'],d['revenue']); plt.title('Completed transaction revenue by day'); plt.xlabel('Date'); plt.ylabel('Revenue (synthetic currency units)'); plt.xticks(rotation=30); save('01_daily_revenue.png')
d=read('category_revenue.csv').head(10).sort_values('revenue'); plt.figure(figsize=(10,5)); plt.barh(d['category_name'].fillna('Unknown'),d['revenue']); plt.title('Top categories by completed net revenue'); plt.xlabel('Revenue (synthetic currency units)'); save('02_categories.png')
d=read('top_products.csv').head(10).sort_values('revenue'); plt.figure(figsize=(10,5)); plt.barh(d['product_id'],d['revenue']); plt.title('Top 10 products by completed net revenue'); plt.xlabel('Revenue (synthetic currency units)'); save('03_products.png')
d=read('funnel.csv'); plt.figure(figsize=(10,4)); plt.barh(d['stage'][::-1],d['sessions'][::-1]); plt.title('Cumulative session conversion funnel'); plt.xlabel('Sessions'); save('04_funnel.png')
d=read('customer_segments.csv'); plt.figure(figsize=(7,4)); plt.bar(d['segment'],d['customers']); plt.title('Completed-purchase customer segments'); plt.ylabel('Customers'); save('05_segments.png')
d=read('device_referrer.csv'); d=d.groupby('device',as_index=False)[['sessions','completed_purchases']].sum(); d['conversion_pct']=100*d['completed_purchases']/d['sessions']; plt.figure(figsize=(7,4)); plt.bar(d['device'].fillna('Unknown'),d['conversion_pct']); plt.title('Completed-purchase conversion by device'); plt.ylabel('Conversion rate (%)'); save('06_device_conversion.png')
print('Created 6 charts in',out.resolve())
