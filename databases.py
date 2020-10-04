import psycopg2
import cx_Oracle
import sqlite3
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np


class Postgresql:

    def create_table(self, query,user, password, host, port, database, connect = True):

        try:
            connection = psycopg2.connect(user = user ,
                                    password = password ,
                                    host = host ,
                                    port = port ,
                                    database = database )

            cursor = connection.cursor()

            create_query = query
            cursor.execute(create_query)
            connection.commit()
            print("Tabela criada no postgresql com sucesso ")

        except (Exception, psycopg2.DatabaseError) as error :
            print ("Erro durante a criação da tabela ", error)

        finally:

                if connect:
                    cursor.close()
                    connection.close()
                    print("Conexão com Postgresql fechada")

    def retrieve_data(self,query, user, password, host, port, database, connect = True, objeto = 'pd'):

        try:
            connection = psycopg2.connect(user = user ,
                                    password = password ,
                                    host = host ,
                                    port = port ,
                                    database = database )

            cursor = connection.cursor()

            select_query = query

            cursor.execute(select_query)
            print("Buscando os dados!!!")

            resultado = cursor.fetchall()
            colunas = [desc[0] for desc in cursor.description]

            if objeto == 'pd':
                base = pd.DataFrame(resultado, columns=colunas)
            else:
                base = np.array(resultado)
            return base

        except (Exception, psycopg2.DatabaseError) as error :
            print ("Erro durante a querry", error)

        finally:

                if connect:
                    cursor.close()
                    connection.close()
                    print("Conexão com Postgresql fechada")


    def insert_data(self, df, tabela, user, password, host, port, database, connect = True):

        try:

            connection = psycopg2.connect(user = user ,
                                    password = password ,
                                    host = host ,
                                    port = port ,
                                    database = database )
            cursor = connection.cursor()


            cols=",".join([str(i) for i in df.columns.tolist()])

            for _,row in df.iterrows():
                sql = "INSERT INTO" + tabela + "(" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                connection.commit()

        except (Exception, psycopg2.Error) as error :
            if connection:
                print("Erro durante a criação da tabela ", error)

        finally:

            if connect:
                cursor.close()
                connection.close()
                print("Conexão com Postgresql fechada")


class Mysql:

    def create_table(self, query, user, password, host, database, connect=True):

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }

        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()
            create_query = query

            cursor.execute(create_query)
            print('Tabela criada no Mysql com sucesso')

        except mysql.connector.Error as err:
            print(f'Tabela não foi criada devido ao erro {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                print('Conexão fechada com o Mysql')


    def retrieve_data(self, query, user, password, host, database, objeto='pd', connect=True):

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }

        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()
            create_query = query

            cursor.execute(create_query)
            print('Buscando os dados!!!')

            dados = cursor.fetchall()
            colunas = [desc[0] for desc in cursor.description]

            if objeto == 'pd':
                base = pd.DataFrame(dados, columns=colunas)
            else:
                base = np.array(dados)
            return base

        except mysql.connector.Error as err:
            print(f'Erro durante a query {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                print('Conexão fechada com o Mysql')


    def insert_data(self,df ,tabela , user, password, host, database, connect=True):


        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }


        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()

            cols = ",".join([str(i) for i in df.columns.tolist()])

            for _, row in df.iterrows():
                sql = "INSERT INTO" + tabela + "(" + cols + ") VALUES (" + "%s, "*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                cnx.commit()

        except mysql.connector.Error as err:
            print(f'Erro durante a query {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                print('Conexão fechada com o Mysql')




class Sqlite:

    def create_table(self, query, database, connect=True):

        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            cursor.execute(query)
            conn.commit()

        except sqlite3.Error as e:
            print(f'O erro encontrado foi {e}')

        finally:

            if connect:
                conn.close()
                print('Conexão encerrada!!!')

    def retrieve_data(self, query, database, objeto = 'pd', connect=True):

        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            cursor.execute(query)
            conn.commit()
            print('Buscando os dados!!!')

            dados = cursor.fetchall()
            colunas = [desc[0] for desc in cursor.description]

            if objeto == 'pd':
                base = pd.DataFrame(dados, columns=colunas)
            else:
                base = np.array(dados)
            return base


        except sqlite3.Error as e:
            print(f'O erro encontrado foi {e}')

        finally:

            if connect:
                conn.close()
                print('Conexão encerrada!!!')

    def insert_data(self, df, tabela, database, connect=True):

        try:
            conn = sqlite3.connect(database)
            df.to_sql(tabela, conn, if_exists='replace', index=False)

        except sqlite3.Error as e:
            print(f'O erro encontrado foi {e}')

        finally:

            if connect:
                conn.close()
                print('Conexão encerrada')

