import polars as pl
from datetime import datetime

ENDERECO_DADOS = r'./Dados/'

try:

    df_dados = pl.read_csv(ENDERECO_DADOS + 'dados_teste.csv', separator=',', encoding='utf-8')

    df_dados_lazy = df_dados.lazy()

    df_dados_lazy = (
        df_dados_lazy
        .filter(pl.col('regiao') == 'SP')
        .group_by('forma_pagamento')
        .agg((pl.col('total_vendas').sum())))
    
    df_final = df_dados_lazy.collect()

    print(df_final)
    
except ImportError as e:
    print(f'Erro ao obter dados: {e}')