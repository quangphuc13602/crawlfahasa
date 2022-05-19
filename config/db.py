from sqlalchemy import create_engine, MetaData
import config.db_account as account

# engine = create_engine("mysql+pymysql://root:123456@localhost:3306/fahasadb")
# engine = create_engine("mysql+mysqlconnector://root:phamvandong@localhost:3306/fahasadb")
string_connect = "postgresql://fwvmiexjrypmnc:f02d92ba2e2218244b25a2916375d26c0493c967c200de471cc830bb708fffc5@ec2-54-165-184-219.compute-1.amazonaws.com:5432/d8eqldbr7681kv"

engine = create_engine(string_connect)

meta = MetaData()

conn = engine.connect()