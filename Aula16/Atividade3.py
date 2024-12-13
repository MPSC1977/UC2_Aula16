import polars as pl
from datetime import datetime

ENDERECO_DADOS = r'./Dados/'

try:

    df_dados = pl.read_csv(ENDERECO_DADOS + 'dados_teste.csv', separator=',', encoding='utf-8')

    df_dados_lazy = df_dados.lazy()

    df_dados_lazy = (
        df_dados_lazy
        # .filter(pl.col('preco') > 1000)
        .group_by(['regiao', 'forma_pagamento'])
        .agg((pl.col('produto').value_counts().first().alias('produtos_mais_vendidos'), pl.col('forma_pagamento').value_counts().first().alias('pg_mais_usado'), pl.col('total_vendas').mean().alias('média_de_vendas'))))
    
    df_final = df_dados_lazy.collect()

    print(df_final)
    
except ImportError as e:
    print(f'Erro ao obter dados: {e}')