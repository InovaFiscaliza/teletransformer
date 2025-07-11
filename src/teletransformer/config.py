import pyarrow as pa

ETL_PARTITION_SIZE = "50MB"  # Default partition size for Dask ETL operations

SCHEMA = {
    "DT_CHAMADA": pa.date32(),
    "HORA_CHAMADA": pa.time64("us"),
    "QT_TEMPO_CONVERSACAO": pa.int64(),
}

ERICSSON_TEXT_COLUMNS_TO_READ = {
    # column_name_from_text_file : column_name_to_parquet_file
    "Tipo_de_chamada": "SG_TIPO_CDR",
    "Bilhetador": "CO_BILHETADOR",
    "Data": "DT_CHAMADA",
    "Hora": "HORA_CHAMADA",
    "IMSI": "NU_IMSI",
    "1stCelA": "CO_CGI_CELULA",
    "Outgoing_route": "CO_ROTA_SAIDA",  # não tem no nokia
    "Origem": "NU_ORIGEM",
    "Destino": "NU_DESTINO",
    "Call_position": "CO_COMPLETAMENTO",  # não tem no nokia
    "Translated_number": "NU_TRADUZIDO",  # não tem no nokia
    "IMEI": "NU_IMEI",
    "Arquivobruto": "NO_ARQUIVO",
    "Referencia": "CO_REFERENCIA",
    "TTC": "QT_TEMPO_CONVERSACAO",
}

NOKIA_TEXT_COLUMNS_TO_READ = {
    "TipodeCDR": "SG_TIPO_CDR",
    "Exch_id": "CO_BILHETADOR",
    "DataReferencia": "DT_CHAMADA",
    "IMSI": "NU_IMSI",
    "TecnologiaCelula-MCC-MNC": "TecnologiaCelula-MCC-MNC",
    "LAC": "LAC",
    "1stCellA": "1stCellA",
    "Origem": "NU_ORIGEM",
    "Destino": "NU_DESTINO",
    "NumeroConec.": "NU_CONECTADO",
    "NumeroDig.": "NU_DIGITADO",
    "IMEI": "NU_IMEI",
    "Arquivo": "NO_ARQUIVO",
    "TTC": "QT_TEMPO_CONVERSACAO",
    "Referencia": "CO_REFERENCIA",
}

NOKIA_RECORD_COLUMNS = [
    "SG_TIPO_CDR",
    "CO_BILHETADOR",
    "DT_CHAMADA",
    "HORA_CHAMADA",
    "NU_IMSI",
    "CO_CGI_CELULA",
    "NU_ORIGEM",
    "NU_DESTINO",
    "NU_CONECTADO",
    "NU_DIGITADO",
    "NU_IMEI",
    "NO_ARQUIVO",
    "QT_TEMPO_CONVERSACAO",
    "CO_REFERENCIA",
]
