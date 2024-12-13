import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# obter dados
try:
    print('Obtendo dados...')

    ENDERECO_DADOS = 'https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv'
    
    # utf-8, iso-8859-1, latin1, cp1252
    df_ocorrencias = pd.read_csv(ENDERECO_DADOS, sep=';', encoding='iso-8859-1')
    
    # Demilitando somente as variáveis
    df_lesoes = df_ocorrencias[['cisp', 'lesao_corp_culposa', 'lesao_corp_dolosa']]

    # Totalizar por CISP
    df_total_lesoes = df_lesoes.groupby(['cisp']).sum(['lesao_corp_culposa', 'lesao_corp_dolosa']).reset_index()

    print(df_total_lesoes.head())

    print('Dados obtidos com sucesso!')

except ImportError as e:
    print(f'Erro ao obter dados: {e}')
    exit()

try:
    print('Calculando correlação...')

    correlacao = np.corrcoef(df_total_lesoes['lesao_corp_culposa'], df_total_lesoes['lesao_corp_dolosa'])[0,1]
    print(f'Correlação: {correlacao}')

    plt.scatter(df_total_lesoes['lesao_corp_culposa'], df_total_lesoes['lesao_corp_dolosa'])
    plt.title(f'Correlação: {correlacao}')
    plt.xlabel('Lesão Culposa')
    plt.ylabel('Lesão Dolosa')
    plt.show()

except ImportError as e:
    print(f'Erro ao obter dados: {e}')
    exit()
