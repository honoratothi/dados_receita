import os
from datetime import datetime as dt

import pandas as pd
from pymongo import MongoClient


def inserir_banco(data, base, collection_index):
    inicio = dt.now()
    tamanho = data.shape

    with MongoClient('mongodb://localhost:27017') as conn:
        db = conn['dados_receita']
        dados_receita = db[base]

        data.reset_index(inplace=True, drop=True)
        data = data.to_dict('records')

        dados_receita.insert_one(data.pop())
        for index in collection_index:
            dados_receita.create_index(index)  # todo: trocar pela variável índice
        dados_receita.insert_many(data, ordered=False)
    log = f'tempo de execução:{dt.now() - inicio}; início: {inicio}; fim: {dt.now()};\n ' \
          f'para inserir a collection {base} com {tamanho[0]} linhas e {tamanho[1]} colunas.'
    print(log)


path = r'D:\alura_cursos\dados_receita'

collection_key = {'Motivos': ['codigo'],
                  'Paises': ['codigo'],
                  'Qualificacoes': ['codigo'],
                  'Naturezas': ['codigo'],
                  'Municipios': ['codigo'],
                  'Cnaes': ['codigo'],
                  'Empresas': ['cnpj_radical'],
                  'Estabelecimentos': ['cnpj_radical', 'cnpj'],
                  # todo: definir cnpj completo para a collection estabelecimentos
                  'Simples': ['cnpj_radical'],
                  'Socios': ['cnpj_radical', 'socio_cpf_cnpj']
                  }
# todo: avaliar uso do arquivo motivos
file_type_list = {'Motivos': ['codigo', 'descricao_motivo'],
                  'Paises': ['codigo', 'nome'],
                  'Qualificacoes': ['codigo', 'descricao'],
                  'Naturezas': ['codigo', 'descricao'],
                  'Municipios': ['codigo', 'descricao'],
                  'Cnaes': ['codigo', 'descricao'],
                  'Empresas': ['cnpj_radical', 'razao_social', 'natureza', 'qualificacao_responsavel',
                               'capital_social', 'porte_cod', 'ente_federativo'],
                  'Estabelecimentos': ['cnpj_radical', 'cnpj_ordem', 'cnpj_digito', 'identificador_matriz_filial',
                                       'nome_fantasia', 'situacao_cadastral', 'situacao_cadastral_data',
                                       'situacao_cadastral_motivo', 'cidade_exterior_nome', 'pais',
                                       'inicio_atividade_data', 'cnae', 'secundario_cnae', 'logradouro_tipo',
                                       'logradouro', 'logradouro_numero', 'logradouro_complemento',
                                       'logradouro_bairro', 'logradouro_cep', 'logradouro_uf', 'logradouro_municipio',
                                       'tel_1_ddd', 'tel_1', 'tel_2_ddd', 'tel_2', 'fax_ddd',
                                       'fax', 'correio_eletronico', 'situacao_especial', 'situacao_especial_data'],
                  'Simples': ['cnpj_radical', 'simples_opcao', 'simples_opcao_data', 'simples_exclusao_data',
                              'mei_opcao', 'mei_opcao_data', 'mei_exclusao_data'],
                  'Socios': ['cnpj_radical', 'socio_identificador', 'socio_nome', 'socio_cpf_cnpj',
                             'socio_qualificacao',
                             'socio_entrada_data', 'pais', 'representante_legal', 'representante_nome',
                             'representante_qualificacao', 'faixa_etaria']
                  }

def importacao_arquivos(item_importacao):
    fields = file_type_list.get(item_importacao)
    files = os.listdir(rf'{path}\{item_importacao}')
    for index, file in enumerate(files):
        file_extension = file[-3:].lower()
        if file_extension == 'csv':
            new_df = pd.read_csv(rf'{path}\{item_importacao}\{file}', delimiter=';',
                                 encoding='mbcs', names=fields, skiprows=1, dtype=str)

            collection_index = collection_key.get(item_importacao)
            new_df.fillna('', inplace=True)
            if item_importacao == 'Estabelecimentos':
                new_df['cnpj'] = new_df.cnpj_radical + new_df.cnpj_ordem + new_df.cnpj_digito
            print(f'inserindo dados na base {item_importacao} {index}....')
            inserir_banco(new_df, item_importacao, collection_index)

if __name__ == '__main__':
    for item in file_type_list.keys():
        importacao_arquivos(item)
