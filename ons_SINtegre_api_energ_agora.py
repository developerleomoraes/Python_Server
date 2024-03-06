import json
import orjson
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
import requests
import pprint
from Webscraping_Furnas.Connector_DataBase import Connector_dataBase
from Webscraping_Furnas.DAO import DAO
from datetime import datetime, timedelta
import time
# from ONS.ons_Carga_Global_api import ONS_API_Carga_Global
from ons_Carga_Global_api import ONS_API_Carga_Global
from reltools import itemgetter


# API url: https://portal-integra.ons.org.br/catalog/default/api/EnergiaAgora/definition

class ONS_SINtegre_api_energ_agora:

    def __init__(self) -> None:
        self.conn = Connector_dataBase()
        self.dao = DAO(None)
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


    # Set Regioes ####################################################################
    def _insert_regioes_balEnerg(self) -> None:
        if(self.dao._count('Regiao') == 0):
            sql_insert_regioes_balEnerg = f"""
                INSERT INTO Regiao (nome) VALUES
                ('sudesteECentroOeste'),
                ('sul'),
                ('nordeste'),
                ('norte'),
                ('internacional'),
                ('intercambio')
            """
            self.dao._insert(sql_insert_regioes_balEnerg)



    # Consult ID #####################################################################
    def consult_id(self, api_data: dict) -> list:
        api_data_keys = list(api_data.keys())
        results = { 'sudesteECentroOeste': None,
                    'sul' : None,
                    'nordeste' : None,
                    'norte' : None,
                    'internacional' : None,
                    'intercambio' : None
            }

        for i in range(1, len(api_data_keys)):
            region_name = api_data_keys[i]
            sql_query_reg_id = f'SELECT id FROM Regiao WHERE nome = %s'

            self.conn.connection_dataBase()
            try:
                cursor = self.conn.connection.cursor()
                cursor.execute(sql_query_reg_id, (region_name,))
                result = cursor.fetchone()
                results[region_name] = result[0]

            except Exception as e:
                self.conn.connection.rollback()
                print(e)

            finally:
                cursor.close()
                self.conn.connection.close()
                print('Close Connection!')

        return results




    # Avoid double data in Database
    def _query_avoid_double_data(self, response_data: dict) -> list[str]:
        check_avoid_data = []
        sql_query_avoid_data = f"""
            SELECT data_carga FROM ONS_SINtegre_balanco_energetico_agora ORDER BY data_carga DESC LIMIT 1;
        """

        result_consult = self.dao._consult(sql_query_avoid_data)
        check_avoid_data.append(result_consult)

        data_carga = response_data['Data']
        data_carga = datetime.strptime(data_carga, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        check_avoid_data.append(data_carga)

        return check_avoid_data




    # Route 1 - Energia Agora #############################################################################
    # Endpoint Balanço Energético agora ###################################################################
    def _get_balanco_energ_agora(self) -> dict:

        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT


        url_balanco_energ = 'https://integra.ons.org.br/api/energiaagora/GetBalancoEnergetico/null'

        headers = {
            'accept':'application/json',
            'Authorization': f'bearer {self.access_token}'
        }


        with urllib3.PoolManager(ssl_context=ctx) as http:
            response = http.request(
                            'GET',
                            url_balanco_energ,
                            headers=headers
                        )


            print('\n')
            print('---------------------- Success Request ----------------------')
            print(f'Status Code: {response.status}')


        if response.status == 200:
            try:
                response_data = response.data.decode('utf-8')
                response_data = json.loads(response_data)
                print(response_data)

                check_avoid_data = self._query_avoid_double_data(response_data)

                if check_avoid_data[0] != check_avoid_data[1]:
                    self._set_query_balanco_energ_agora(response_data)
                else:
                    pass


            except Exception as e:
                print(f'Error in JSON request: {e}')



    # Set Query Balanco energetico agora ##################################################################              
    def _set_query_balanco_energ_agora(self, api_data: dict) -> None:
        self._insert_regioes_balEnerg()
        consult_id = self.consult_id(api_data) 
        data_carga = api_data['Data'] 
        print(f'Printando o resultado do consult_id: {consult_id}')

        columns_by_region = {
            'sudesteECentroOeste': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar', 'itaipu50HzBrasil', 'itaipu60Hz'],
            'sul': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar'],
            'nordeste': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar'],
            'norte': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar']   
        }

        internacional_columns = {
            'internacional': ['argentina', 'paraguai', 'uruguai'],
            'intercambio': ['internacional_sul', 'sul_sudeste', 'sudeste_nordeste', 'sudeste_norteFic', 'norte_norteFic', 'norteFic_nordeste']
        }


        consult_id_values = list(consult_id.values())

        for region, columns in columns_by_region.items():
            if region in api_data:
                data_region = api_data[region]['geracao']
                carga_verificada = api_data[region]['cargaVerificada']
                importacao = api_data[region]['importacao']
                exportacao = api_data[region]['exportacao']
                

                sql_str_region = f"""
                    INSERT INTO ONS_SINtegre_balanco_energetico_agora(
                        data_carga,
                        {', '.join(columns + ['cargaVerificada', 'importacao', 'exportacao', 'id_regiao'])}
                    )
                    VALUES (
                        '{(data_carga)}',
                        {', '.join([f"'{data_region[col]}'" for col in columns])},
                        '{(carga_verificada)}',
                        '{importacao}',
                        '{(exportacao)}',
                        '{(consult_id_values.pop(0))}'
                    )
                """

                #sql_queries.append(sql_str_region)

                print(f'\nRegião {region}')
                # print(sql_str_region)
                self.dao._insert(sql_str_region)


        for key, columns in internacional_columns.items():
            if key in api_data:
                data_key = api_data[key]
                sql_str_inter = f"""
                    INSERT INTO ONS_SINtegre_balanco_energetico_agora(
                        data_carga,
                        {', '.join(columns + ['id_regiao'])}
                    )
                    VALUES (
                        '{(data_carga)}',
                        {', '.join([f"'{data_key[col]}'" for col in columns])},
                        '{(consult_id_values.pop(0))}'
                    )
                """

                print(f'\nKey: {key}')
                #print(sql_str_inter)
                self.dao._insert(sql_str_inter)


            




    # Route 2 #############################################################################################
    # Endpoint Balanço Energético agora ###################################################################
    def _get_balanco_energ_consolidado_agora(self) -> dict:

        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT


        url_balanco_energ = 'https://integra.ons.org.br/api/energiaagora/GetBalancoEnergeticoConsolidado/null'

        headers = {
            'accept':'application/json',
            'Authorization': f'bearer {self.access_token}'
        }


        with urllib3.PoolManager(ssl_context=ctx) as http:
            response = http.request(
                            'GET',
                            url_balanco_energ,
                            headers=headers
                        )


            print('\n')
            print('---------------------- Success Request ----------------------')
            print(f'Status Code: {response.status}')


        if response.status == 200:
            try:
                response_data = response.data.decode('utf-8')
                response_data = json.loads(response_data)
                print(response_data)


                check_avoid_data = self._query_avoid_double_data(response_data)

                if check_avoid_data[0] != check_avoid_data[1]:
                    self._set_query_balanco_energ_consolidado_agora(response_data)
                else:
                    pass


            except Exception as e:
                print(f'Error in JSON request: {e}')




    # Set Query Balanco energetico consolidado agora ######################################################
    def _set_query_balanco_energ_consolidado_agora(self, api_data: dict) -> str:
        self._insert_regioes_balEnerg()
        consult_id = self.consult_id(api_data)
        print(f'Printando o resultado do consult_id: {consult_id}')
        data_carga = api_data['Data']
        print(data_carga)


        columns_by_region = {
            'sudesteECentroOeste': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar', 'itaipu50HzBrasil', 'itaipu60Hz'],
            'sul': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar'],
            'nordeste': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar'],
            'norte': ['total', 'hidraulica', 'termica', 'eolica', 'nuclear', 'solar']   
        }
        

        internacional_columns = {
            'internacional': ['NULL'],
            'intercambio': ['internacional_sul', 'sul_sudeste', 'sudeste_nordeste', 'sudeste_norteFic', 'norte_norteFic', 'norteFic_nordeste']
        }
        
        consult_id_values = list(consult_id.values())

        for region, columns in columns_by_region.items():
            if region in api_data:
                data_region = api_data[region]['geracao']
                carga_verificada = api_data[region]['cargaVerificada']
                importacao = api_data[region]['importacao']
                exportacao = api_data[region]['exportacao']

                sql_str_region = f"""
                    INSERT INTO ONS_SINtegre_balanco_energetico_agora(
                        data_carga,
                        {', '.join(columns + ['cargaVerificada', 'importacao', 'exportacao', 'id_regiao'])}
                    )
                    VALUES (
                        '{(data_carga)}',
                        {', '.join([f"'{data_region[col]}'" for col in columns])},
                        '{(carga_verificada)}',
                        '{importacao}',
                        '{(exportacao)}',
                        '{(consult_id_values.pop(0))}'
                    )
                """

                print(f'\nRegião {region}')
                print(sql_str_region)

       

        for key, columns in internacional_columns.items():
            if key in api_data:
                data_key = api_data[key]
                sql_str_inter = f"""
                INSERT INTO ONS_SINtegre_balanco_energetico_agora(
                    data_carga,
                    {', '.join(columns + ['id_regiao'])}
                )
                VALUES (
                    '{(data_carga)}',
                    {', '.join([f"'{data_key[col]}'" for col in columns])},
                    '{(consult_id_values.pop(0))}'
                )
            """
                
            print(f'\nRegião {region}')
            print(sql_str_inter)
                    


    #    for key, value in api_data.items():
    #         if key == 'Data':
    #             pass

    #         elif key == 'sudesteECentroOeste':
    #             data_sudesteECentroOeste = value['geracao']
    #             cargaVerificada = value['cargaVerificada']
    #             importacao = value['importacao']
    #             exportacao = value['exportacao']

    #             sql_str_sudesteECentroOeste = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     '{(data_sudesteECentroOeste['total'])}',
    #                     '{(data_sudesteECentroOeste['hidraulica'])}',
    #                     '{(data_sudesteECentroOeste['termica'])}',
    #                     '{(data_sudesteECentroOeste['eolica'])}',
    #                     '{(data_sudesteECentroOeste['nuclear'])}',
    #                     '{(data_sudesteECentroOeste['solar'])}',
    #                     '{(data_sudesteECentroOeste['itaipu50HzBrasil'])}',
    #                     '{(data_sudesteECentroOeste['itaipu60Hz'])}',
    #                     '{(cargaVerificada)}',
    #                     '{(importacao)}',
    #                     '{(exportacao)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(consult_id[0])}'
    #                 )
    #             """

    #             print('\n')
    #             print('Região sudesteECentroOeste')
    #             # print(sql_str_sudesteECentroOeste)
    #             self.dao._insert(sql_str_sudesteECentroOeste)
                


    #         elif key == 'sul':
    #             data_sul = value['geracao']
    #             cargaVerificada = value['cargaVerificada']
    #             importacao = value['importacao']
    #             exportacao = value['exportacao']

    #             sql_str_sul = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     '{(data_sul['total'])}',
    #                     '{(data_sul['hidraulica'])}',
    #                     '{(data_sul['termica'])}',
    #                     '{(data_sul['eolica'])}',
    #                     '{(data_sul['nuclear'])}',
    #                     '{(data_sul['solar'])}',
    #                     NULL,
    #                     NULL,
    #                     '{(cargaVerificada)}',
    #                     '{(importacao)}',
    #                     '{(exportacao)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(consult_id[1])}'
    #                 )
    #             """

    #             print('\n')
    #             print('Região sul')
    #             # print(sql_str_sul)
    #             self.dao._insert(sql_str_sul)



    #         elif key == 'nordeste':
    #             data_nordeste = value['geracao']
    #             cargaVerificada = value['cargaVerificada']
    #             importacao = value['importacao']
    #             exportacao = value['exportacao']

    #             sql_str_nordeste = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     '{(data_nordeste['total'])}',
    #                     '{(data_nordeste['hidraulica'])}',
    #                     '{(data_nordeste['termica'])}',
    #                     '{(data_nordeste['eolica'])}',
    #                     '{(data_nordeste['nuclear'])}',
    #                     '{(data_nordeste['solar'])}',
    #                     NULL,
    #                     NULL,
    #                     '{(cargaVerificada)}',
    #                     '{(importacao)}',
    #                     '{(exportacao)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(consult_id[2])}'
    #                 )
    #             """


    #             print('\n')
    #             print('Região Nordeste')
    #             # print(sql_str_nordeste)
    #             self.dao._insert(sql_str_nordeste)



    #         elif key == 'norte':
    #             data_norte = value['geracao']
    #             cargaVerificada = value['cargaVerificada']
    #             importacao = value['importacao']
    #             exportacao = value['exportacao']

    #             sql_str_norte = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     '{(data_norte['total'])}',
    #                     '{(data_norte['hidraulica'])}',
    #                     '{(data_norte['termica'])}',
    #                     '{(data_norte['eolica'])}',
    #                     '{(data_norte['nuclear'])}',
    #                     '{(data_norte['solar'])}',
    #                     NULL,
    #                     NULL,
    #                     '{(cargaVerificada)}',
    #                     '{(importacao)}',
    #                     '{(exportacao)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(consult_id[3])}'
    #                 )
    #             """

    #             print('\n')
    #             print('Região norte')
    #             # print(sql_str_norte)
    #             self.dao._insert(sql_str_norte)


    #         elif key == 'internacional':
    #             sql_str_internacional = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(consult_id[4])}'
    #                 )
    #             """

    #             print('\n')
    #             print('Internacional')
    #             # print(sql_str_internacional)
    #             self.dao._insert(sql_str_internacional)



    #         elif key == 'intercambio':
    #             sql_str_intercambio = f"""
    #                 INSERT INTO ONS_SINtegre_balanco_energ_consolidado_agora(
    #                     data_carga,
    #                     total,
    #                     hidraulica,
    #                     termica,
    #                     eolica,
    #                     nuclear,
    #                     solar,
    #                     itaipu50HzBrasil,
    #                     itaipu60Hz,
    #                     cargaVerificada,
    #                     importacao,
    #                     exportacao,
    #                     internacional,
    #                     internacional_sul,
    #                     sul_sudeste,
    #                     sudeste_nordeste,
    #                     sudeste_norteFic,
    #                     norte_norteFic,
    #                     norteFic_nordeste,
    #                     id_regiao
    #                 )
    #                 VALUES (
    #                     '{(data_carga)}',
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     NULL,
    #                     '{(value['internacional_sul'])}',
    #                     '{(value['sul_sudeste'])}',
    #                     '{(value['sudeste_nordeste'])}',
    #                     '{(value['sudeste_norteFic'])}',
    #                     '{(value['norte_norteFic'])}',
    #                     '{(value['norteFic_nordeste'])}',
    #                     '{(consult_id[5])}'
    #                 )
    #             """
                

    #             print('\n')
    #             print('Intercambio')
    #             # print(sql_str_intercambio)
    #             self.dao._insert(sql_str_intercambio)
                


    
    



if __name__ == '__main__':
    obj = ONS_SINtegre_api_energ_agora()
    obj._get_balanco_energ_consolidado_agora()
    # obj._get_balanco_energ_agora()
    
  
    