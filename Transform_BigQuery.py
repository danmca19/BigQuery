from google.cloud import bigquery
import pandas as pd

def bigquery_io(project_id, dataset_id, table_id, operation, df=None, if_exists="replace"):
    """
    Realiza operações de leitura e escrita no BigQuery.

    Args:
        project_id (str): O ID do projeto do Google Cloud.
        dataset_id (str): O ID do conjunto de dados do BigQuery.
        table_id (str): O ID da tabela do BigQuery.
        operation (str): 'read' para ler dados ou 'write' para escrever dados.
        df (pandas.DataFrame, opcional): O DataFrame a ser salvo (apenas para 'write').
        if_exists (str, opcional): O que fazer se a tabela já existir (apenas para 'write').
            Pode ser 'replace' (substituir), 'append' (adicionar) ou 'fail' (falhar).
            O padrão é 'replace'.

    Returns:
        pandas.DataFrame: Um DataFrame contendo os dados da tabela do BigQuery (apenas para 'read').
        None: Se ocorrer um erro ou se a operação for 'write'.
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        if operation.lower() == "read":
            query_job = client.query(f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`")
            results = query_job.result()
            return results.to_dataframe(create_bqstorage_client=False)

        elif operation.lower() == "write":
            if df is None:
                raise ValueError("DataFrame 'df' é necessário para a operação de escrita.")

            if if_exists.lower() == "replace":
                write_disposition = "WRITE_TRUNCATE"
            elif if_exists.lower() == "append":
                write_disposition = "WRITE_APPEND"
            elif if_exists.lower() == "fail":
                write_disposition = "WRITE_EMPTY"
            else:
                raise ValueError(f"Valor inválido para if_exists: {if_exists}. Deve ser 'replace', 'append' ou 'fail'.")

            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"DataFrame salvo com sucesso em {project_id}.{dataset_id}.{table_id}")
            return None

        else:
            raise ValueError(f"Operação inválida: {operation}. Deve ser 'read' ou 'write'.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None


# Exemplo de uso:
project_id = "ivory-enigma-402100"
dataset_id = "FirstSchema"
table_read_id = "aggregated_complaints"
table_write_id = "status_closed"

# Ler dados do BigQuery
df = bigquery_io(project_id, dataset_id, table_read_id, "read")

if df is not None:
    print("DataFrame lido com sucesso!")
    print(df.head())
    print(df.info())

    # Filtrar dados
    df_closed = df[df["status"].str.lower() == "closed"]

    # Escrever dados filtrados no BigQuery
    bigquery_io(project_id, dataset_id, table_write_id, "write", df=df_closed, if_exists="replace")
    bigquery_io(project_id, dataset_id, table_write_id, "write", df=df_closed, if_exists="append")
    bigquery_io(project_id, dataset_id, table_write_id, "write", df=df_closed, if_exists="fail")

else:
    print("Falha ao ler o DataFrame.")
