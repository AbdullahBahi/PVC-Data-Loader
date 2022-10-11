import pyodbc
import pandas as pd
from openpyxl import load_workbook
import datetime

class SqlConnector():
    def __init__(self, server_name, database_name, user_name, password):
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password

    def sql_to_df(self, query):
        """
        reads data from sql server and loads it to pandas dataframe
        """
        config = 'DRIVER={ODBC Driver 17 for SQL Server};'+'SERVER='+self.server_name+';DATABASE='+self.database_name+';UID='+self.user_name+';PWD='+self.password+';Trusted_Connection=yes;'
        conn = pyodbc.connect(config)
        df = pd.read_sql_query(query, conn)
        return df

    def sql_to_excel(self, query, file_path, sheet_name='sheet1'):
        """
        reads data from sql server and loads it to pandas dataframe
        """
        config = 'DRIVER={ODBC Driver 17 for SQL Server};'+'SERVER='+self.server_name+';DATABASE='+self.database_name+';UID='+self.user_name+';PWD='+self.password+';Trusted_Connection=yes;'
        conn = pyodbc.connect(config)
        df = pd.read_sql_query(query, conn)
        df.to_excel(file_path, sheet_name=sheet_name)
        return df

    def excel_to_sql(self, file_path, sql_table_name):
        """
        writes pandas dataframes to sql server database.
        """
        data = pd.read_excel(file_path)
        data['Date'] = [datetime.datetime.strftime(x, format='%Y-%m-%d %H:%M:%S') for x in data.Date]
        columns = tuple(data.columns)
        config = 'DRIVER={ODBC Driver 17 for SQL Server};'+'SERVER='+self.server_name+';DATABASE='+self.database_name+';UID='+self.user_name+';PWD='+self.password+';Trusted_Connection=yes;'
        conn = pyodbc.connect(config)
        cursor = conn.cursor()
        start = datetime.datetime.now()
        for i in data.index:
            values = tuple(data.iloc[i])
            cursor.execute(f'''
                            INSERT INTO {sql_table_name} {str(columns)}
                            '''.replace('\'', '\"')
                            + f'''
                            VALUES
                            {str(values)}
                            ''')
            conn.commit()
        end = datetime.datetime.now()
        conn.close()
        print('All records were added sucessfully!')
        print(f'Estimated upload speed [record/sec] = {int(len(data)/(end-start).seconds)}')