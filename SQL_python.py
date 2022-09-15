import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# Postgres username, password, and database name
POSTGRES_ADDRESS = 'localhost' ## INSERT YOUR DB ADDRESS IF IT'S NOT ON PANOPLY
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'postgres' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = 'DBkapasshai' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD 
POSTGRES_DBNAME = 'postgres' ## CHANGE THIS TO YOUR DATABASE NAME
# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
                .format(username=POSTGRES_USERNAME,
                        password=POSTGRES_PASSWORD,
                        ipaddress=POSTGRES_ADDRESS,
                        port=POSTGRES_PORT,
                        dbname=POSTGRES_DBNAME))
# Create the connection
cnx = create_engine(postgres_str)

## Input the desired period of years you want to analyse
min_year = 1980
max_year = 2020


# fetch the relevant data from postgreSQL
df = pd.read_sql_query('''SELECT * from random.get_inf_gold_data({year1},{year2});'''.format(year1=min_year ,year2=max_year), cnx)


# Gold Unit:1 Troy Ounce
df_inflation = df[["year","dollar","gold_price","Gold-Dollar"]]
df_inflation = df_inflation.set_index("year")
df_inflation = df_inflation.sort_index()

# Plot Inflation adjusted dollar value
plt.plot(df["year"], df["dollar"], color='forestgreen',lw=2)

# Plot Gold value
plt.plot(df["year"], df["gold_price"], color='goldenrod',lw=2)

plt.xticks(range(min_year, max_year+1,5))
plt.ylim(ymin=0)

plt.show()