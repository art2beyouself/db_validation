import great_expectations as ge

from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint



context = ge.get_context()
my_postgres_datasource = "my_datasource"

# Получаем список всех таблиц
datasource_config = {
    "name": f"{my_postgres_datasource}",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "CONNECT_TO_POSTGRES",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}

# Выгрузить конкретную таблицу
batch_request = BatchRequest(
    datasource_name=f"{my_postgres_datasource}",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="public.suppliers_stage",  # таблица для запроса выгрузки на валидацию данных
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)


