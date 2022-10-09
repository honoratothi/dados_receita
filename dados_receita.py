import pandas as pd
import os

path = r'D:\alura_cursos\dados_receita'

file_type_list = {'Paises': ['codigo', 'nome'],
                  'Qualificacoes': ['codigo', 'descricao'],
                  'Naturezas': ['codigo', 'descricao'],
                  'Municipios': ['codigo', 'descricao'],
                  'Cnaes': ['codigo', 'descricao'],
                  'Empresas': ['cnpj_raiz', 'razao_social', 'natureza', 'qualificacao_responsavel', 'capital_social', 'porte_cod', 'ente_federativo']
                  }

'''
, 'socios', 'empresas', 'estabelecimentos', 'motivos', }
'''

def importacao_arquivos(item_importacao):
    fields = file_type_list.get(item_importacao)
    files = os.listdir(rf'{path}\{item_importacao}')
    df = pd.DataFrame()
    for file in files:
        file_extension = file[-3:].lower()
        if file_extension == 'csv':
            print(item_importacao, ' - ', file)
            new_df = pd.read_csv(rf'{path}\{item_importacao}\{file}', delimiter=';',
                 encoding='mbcs', names=fields)
            df = pd.concat([df, new_df])
    print(df.head(), df.shape, len(df))
    df.head().to_csv(rf'{path}\{item_importacao}.csv', index_label='índice')
pd.set_option('display.max_columns', 25)

if __name__ == '__main__':
    for item in file_type_list.keys():
        importacao_arquivos(item)

