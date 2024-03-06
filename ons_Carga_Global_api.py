
import requests
import pprint
from datetime import datetime, timedelta
from Webscraping_Furnas.Connector_DataBase import Connector_dataBase
from Webscraping_Furnas.Debugging import Debugging
from Webscraping_Furnas.DAO import DAO
from reltools import itemgetter

# URL: http://ons-dl-prod-opendata-swagger.s3-website-us-east-1.amazonaws.com/


class ONS_API_Carga_Global:
    def __init__(self) -> None:
        self.conn = Connector_dataBase()
        self.dat_inicio = None
        self.dat_fim = None


    # Get API Verificada ##################################################################################################
    def _get_carga_verificada(self, cod_areacarga: str) -> None:
        sql_query = f"""
            SELECT dat_referencia FROM ONS_Carga_Verificada ORDER BY dat_referencia DESC LIMIT 1;
        """
        
        self._set_date_range(sql_query)         
        url_carg_verificada = f'https://apicarga.ons.org.br/prd/cargaverificada?dat_inicio={self.dat_inicio}&dat_fim={self.dat_fim}&cod_areacarga={cod_areacarga}'
        data = requests.get(url_carg_verificada)
        data = data.json()


        for i in data:
            i['din_atualizacao'] = i['din_atualizacao'][:-1]
            i['din_referenciautc'] = i['din_referenciautc'][:-1]



        fields = ('cod_areacarga', 'dat_referencia', 'din_atualizacao', 'din_referenciautc', 'val_cargaglobal', 'val_cargaglobalcons', 'val_cargaglobalsmmgd', 'val_cargammgd', 'val_carganaosupervisionada', 'val_cargasupervisionada', 'val_consistencia')
        # Função para extrair os valores específicos dos dicionários
        tuple_maker = itemgetter(*fields)
        # Convertendo a lista de dicionários em uma lista de tuplas
        tuples_data = list(map(tuple_maker, data))
      
        # Insert data in database
        insert_queries = self._get_insert_query_verificada()
        self.insert_bulk_data(insert_queries, tuples_data)
        
            

    

    # Insert Query #######################################################################################################
    def _get_insert_query_verificada(self) -> str:
        sql_str = f"""
            INSERT INTO ONS_Carga_Verificada(
                cod_areacarga,
                din_atualizacao,
                dat_referencia,
                din_referenciautc,
                val_cargaglobal,
                val_cargaglobalcons,
                val_cargaglobalsmmgd,
                val_cargasupervisionada,
                val_carganaosupervisionada,
                val_cargammgd,
                val_consistencia
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """
        
        return sql_str

    
    

    # Get API Programada ##################################################################################################
    def _get_carga_programada(self, cod_areacarga: str) -> None:

        sql_query = f"""
            SELECT dat_referencia FROM ONS_Carga_Programada ORDER BY dat_referencia DESC LIMIT 1;
        """

        self._set_date_range(sql_query)
        url_carg_programada = f'https://apicarga.ons.org.br/prd/cargaprogramada?dat_inicio={self.dat_inicio}&dat_fim={self.dat_fim}&cod_areacarga={cod_areacarga}'
        data = requests.get(url_carg_programada)
        data = data.json()

        for i in data:
            i['din_referenciautc'] = i['din_referenciautc'][:-1]



        fields = ('cod_areacarga', 'dat_referencia', 'din_referenciautc', 'val_cargaglobalprogramada')
        # Função para extrair os valores específicos dos dicionários
        tuple_maker = itemgetter(*fields)
        # Convertendo a lista de dicionários em uma lista de tuplas
        tuples_data = list(map(tuple_maker, data))
      
        # Insert data in database
        insert_queries = self._get_insert_query_programada()
        self.insert_bulk_data(insert_queries, tuples_data)
    
        


    def _get_insert_query_programada(self) -> list:
            sql_str = f"""
                INSERT INTO ONS_Carga_Programada (
                    cod_areacarga,
                    dat_referencia,
                    din_referenciautc,
                    val_cargaglobalprogramada
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
            """
            
            return sql_str
        
        
    
        
    
    # Set data#############################################################################################################
    def _set_date_range(self, sql_query: str) -> None:      

        current_date = datetime.today().date()
        dat_inicio = self._consult_ONS(sql_query)
    

        if dat_inicio == None:
            self.dat_inicio = current_date
            self.dat_fim = current_date
        elif dat_inicio == current_date:
            print('The date in the database and the real date are the same, nothing will be done!')
            pass
        else:
            self.dat_inicio = dat_inicio + timedelta(days = 1)
            self.dat_fim = current_date - timedelta(days = 1)
        

    # Consult in Database ##################################################################################################
    def _consult_ONS(self, query: str) -> str:
        self.conn.connection_dataBase()
        try:
            cursor = self.conn.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            

        except Exception as e:
            self.conn.connection.rollback()
            print(e)

        finally:
            cursor.close()
            self.conn.connection.close()
            print('Conexão fechada com sucesso!')

        return result[0]


    # Insert Bulk Data
    def insert_bulk_data(self, query: str, data: list) -> None:
        print(query)
        self.conn.connection_dataBase()
        try:
            # Creating a cursor object using the cursor() method
            cursor = self.conn.connection.cursor()
            # Executing the SQL command
            cursor.executemany(query, data)

            # Commit your changes in the database
            self.conn.connection.commit()
            print('Commit da transação')

        except Exception as e:
            # Rolling back in case of error
            self.conn.connection.rollback()
            print(e)

        # Closing the connection
        cursor.close()
        self.conn.connection.close()
        print('Conexão fechada com sucesso!')
