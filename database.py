# 引入需要用的库
import json
import psycopg2
from configparser import ConfigParser
import pandas as pd

# 创建数据库类。

class database:
# initial state 初始化库 内置参数filename，filename应是配置文件名。
    def __init__(self, filename):
        self.filename = filename

# 创建read_config方法 内置参数section，section可以是配置文件内任何section。
    def read_config(self, section):
        parser = ConfigParser()
        parser.read(self.filename)

        db = {}

        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {} has not found in the {} file'.format(section, self.filename))

        return db
# 创建read_values方法， 功能是读取json配置文件中英文列名和中文列名的映射关系（映射关系会被提供） 实现将输出的列名变为中文这一功能。
    def read_values(self, file = '/Users/derrickchen/Documents/圆心科技/project_1 simplify/Json2'):


        with open(file) as jsonFile:
            data = json.load(jsonFile)

        return data

# 数据库连接。
    def connect(self):
        conn = None
        try:
            params = self.read_config('database')
            print("Connecting to the PostgreSQL database...")
            conn = psycopg2.connect(**params)

            cur = conn.cursor()
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            print(db_version)
            conn.close()

        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed!')

# 创建查询需要的信息功能，根据业务的信息需求，从数据库中查询需要的信息。
    def get_needed_info(self):
        conn = None
        lis = []

        params = self.read_config('database')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        columnMapping = self.read_values()
        for column in self.read_config('business')['columns'].split(','):
            column_description = columnMapping.get(str.strip(column))
            if column == None:
                column_description = column
            lis.append(column + ' ' + column_description)

        SQL = "Select " + ",".join(lis) + " from ads_dws.hmb_ads_dws_ads_dws_dm_hmb_column where 1=1 " + self.read_config('business')['select_condition']
        #print(SQL)
        cur.execute(SQL)
        query = pd.read_sql_query(SQL, conn)
        df = pd.DataFrame(query)

        k = cur.rowcount // int(self.read_config('business')['numbers'])
        size = int(self.read_config('business')['numbers'])

        for i in range(k):
            dframe = df[size * i : size * (i + 1)]
            dframe.to_csv(f'列信息_{i + 1}.csv', index = False)


        if  conn is not None:
            cur.close()
            conn.close()




j = database('config2.ini')
j.get_needed_info()

