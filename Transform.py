
import pandas as pd

df = pd.read_csv('./products.csv', names=['Title','store','price','rating','NumberOfReviews'],index_col=None)
for index in df.index:
    df.loc[index, 'store'] = df.loc[index, 'store'][20:]
    df.loc[index,'rating'] = df.loc[index,'rating'].replace(' sur ','/').replace('étoiles', '').strip()
    if df.loc[index, 'rating'] == 'Previouspage' : 
        df.loc[index, 'rating'] = 'None'
    df.loc[index,'NumberOfReviews'] = df.loc[index,'NumberOfReviews'].replace(' évaluations', '')

df.to_csv('/home/aya/VSworkplace/products_transformed.csv')
df.to_json('/home/aya/VSworkplace/products_transformed.json')
    



