import json
import orjson
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
import requests
import pprint
from Webscraping_Furnas.Connector_DataBase import Connector_dataBase
from Webscraping_Furnas.DAO import DAO
from datetime import datetime, timedelta
from ONS.ons_Carga_Global_api import ONS_API_Carga_Global
from reltools import itemgetter


# API URL: https://portal-integra.ons.org.br/api-docs


class ONS_SINtegre_api_carga:

    def __init__(self) -> None:
        self.conn = Connector_dataBase()
        self.ons_carga_global = ONS_API_Carga_Global()
        self.dat_referencia_ini = None
        self.dat_referencia_fim = None

        # Get Token API
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT

        url = 'https://integra.ons.org.br/api/autenticar'

        payload = json.dumps({
            "usuario": "leonardo.moraes@cenergel.com.br",
            "senha": "Cenergelwsfurnas.,.918273"
        })

        headers = {
            'Origin': 'https://portal-integra.ons.org.br',
            'Content-Type': 'application/json'
        }

        with urllib3.PoolManager(ssl_context=ctx) as http:
            response = http.request('POST', url, body=payload, headers=headers)
            print(response.status)
            print(json.loads(response.data.decode('utf-8'))['access_token'])
            self.access_token = json.loads(response.data.decode('utf-8'))['access_token']



    # Endpoint carga verificada por área ##################################################################
    def _get_cargav(self, cod_areacarga: str) -> None:

        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT

        sql_query = f"""
            SELECT dat_referencia FROM ONS_SINtegre_carga_cargav ORDER BY dat_referencia LIMIT 1;
        """

        self._set_data_range(sql_query)
        url_cargv = f'https://integra.ons.org.br/api/cargaglobal/cargav?cod_areacarga={cod_areacarga}&dat_referencia_ini={self.dat_referencia_ini}&dat_referencia_fim={self.dat_referencia_fim}'


        headers = {
            'accept':'application/json',
            'Authorization': f'bearer {self.access_token}'
        }

        with urllib3.PoolManager(ssl_context=ctx) as http:
            response = http.request(
                            'GET', 
                            url_cargv, 
                            headers=headers
                        )


            print('\n')
            print('---------------------- Success Request ----------------------')
            print(f'Status Code: {response.status}')

            if response.status == 200:
                try:
                    response_data = response.data.decode('utf-8')
                    response_data = json.loads(response_data)
                   
                    for i in response_data:
                        i['din_referenciautc'] = i['din_referenciautc'][:-1]

                    fields = (  'cod_areacarga',
                                'dat_referencia',
                                'din_referenciautc',
                                'val_cargaglobal',
                                'val_cargaglobalsmmg',
                                'val_cargammgd',
                                'val_cargaglobalcons',
                                'val_consistencia',
                                'val_cargarvd',
                                'val_cargasup',
                                'val_cargansup'
                            )

                    # Função para extrair os valores específicos dos dicionários
                    tuple_maker = itemgetter(*fields)
                    # Convertendo a lista de dicionários em uma lista de tuplas
                    tuples_data = list(map(tuple_maker, response_data))

                    insert_query = self._set_query_cargav()
                    self.ons_carga_global.insert_bulk_data(insert_query, tuples_data)
           

                except json.JSONDecodeError as e:
                    print(f'Error in JSON request: {e}')


    # Set Query ##################################################################
    def _set_query_cargav(self) -> str:
        sql_str = f"""
            INSERT INTO ONS_SINtegre_carga_cargav(
                cod_areacarga,
                dat_referencia,
                din_referenciautc,
                val_cargaglobal,
                val_cargaglobalsmmg,
                val_cargammgd,
                val_cargaglobalcons,
                val_consistencia,
                val_cargarvd,
                val_cargasup,
                val_cargansup
            )
            VALUES(
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
            


                
    # Endpoint carga programada por área ##################################################################
    def _get_cargap(self, cod_areacarga: str) -> list:

        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT

        sql_query = f"""
           SELECT dat_programacao FROM ONS_SINtegre_carga_cargap ORDER BY dat_programacao LIMIT 1; 
        """

        self._set_data_range(sql_query)
        url_cargap = f'https://integra.ons.org.br/api/cargaglobal/cargap?cod_areacarga={cod_areacarga}&dat_referencia_ini={self.dat_referencia_ini}&dat_referencia_fim={self.dat_referencia_fim}'


        headers = {
            'accept':'application/json',
            'Authorization': f'bearer {self.access_token}'
        }

        with urllib3.PoolManager(ssl_context=ctx) as http:
            response = http.request(
                            'GET', 
                            url_cargap, 
                            headers=headers
                        )


            print('\n')
            print('---------------------- Success Request ----------------------')
            print(f'Status Code: {response.status}')

            if response.status == 200:
                try:
                    response_data = response.data.decode('utf-8')
                    response_data = json.loads(response_data)
                

                    for i in response_data:
                        i['din_referenciautc'] = i['din_referenciautc'][:-1]


                    fields = (  'cod_areacarga',
                                'dat_programacao',
                                'din_referenciautc',
                                'val_cargaprogramada'
                            )
                    

                    # Função para extrair os valores específicos dos dicionários
                    tuple_maker = itemgetter(*fields)
                    # Convertendo a lista de dicionários em uma lista de tuplas
                    tuples_data = list(map(tuple_maker, response_data))

                    insert_query = self._set_query_cargap()
                    self.ons_carga_global.insert_bulk_data(insert_query, tuples_data)

           

                except json.JSONDecodeError as e:
                    print(f'Error in JSON request: {e}')




    def _set_query_cargap(self) -> str:
        sql_str = f"""
            INSERT INTO ONS_SINtegre_carga_cargap(
                cod_areacarga,
                dat_programacao,
                din_referenciautc,
                val_cargaprogramada
            )
            VALUES(
                %s,
                %s,
                %s,
                %s
            )
        """

        return sql_str



    # Set data#############################################################################################################
    def _set_data_range(self, sql_query: str) -> None:
        
        current_date = datetime.today().date()
        dat_referencia_ini = self.ons_carga_global._consult_ONS(sql_query)

        if dat_referencia_ini == None:
            self.dat_referencia_ini = current_date
            self.dat_referencia_fim = current_date
        elif dat_referencia_ini == current_date:
            print('The date in the database and the real date are the same, nothing will be done!')
            pass
        else:
            self.dat_referencia_ini = dat_referencia_ini + timedelta(days = 1)
            self.dat_referencia_fim = current_date - timedelta(days = 1)
            
           

    

