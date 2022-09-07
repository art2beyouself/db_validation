from conf import batch_request, SimpleCheckpoint, validator, context


class Validator:

    def validation(self):
        # ----------------- Валидация колонок ----------------- #
        # Проверка id на уникальность id
        validator.expect_column_values_to_be_unique(column="id")
        # Проверка identifier на уникальность identifier
        validator.expect_column_values_to_be_unique(column="identifier")
        # Проверка столбца на тип данных name_kk
        validator.expect_column_values_to_be_of_type(column="name_kk", type_="VARCHAR")
        # Проверка столбца на тип данных name_ru
        validator.expect_column_values_to_be_of_type(column="name_ru", type_="VARCHAR")
        # Проверка на not null country_id
        validator.expect_column_values_to_not_be_null(column="country_id")
        # Проверка на not null begin_date
        validator.expect_column_values_to_not_be_null(column="begin_date")
        # Проверка столбца на тип данных position_kk
        validator.expect_column_values_to_be_of_type(column="position_kk", type_="VARCHAR")
        # Проверка столбца на тип данных position_ru
        validator.expect_column_values_to_be_of_type(column="position_ru", type_="VARCHAR")
        # Проверка email на валидность
        validator.expect_column_values_to_match_regex(column="site", regex="([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+")
        # Проверка столбца на тип данных building
        validator.expect_column_values_to_be_of_type(column="building", type_="INTEGER")
        # Проверка на not null city
        validator.expect_column_values_to_not_be_null(column="city")
        # Проверка столбца на тип данных flat
        validator.expect_column_values_to_be_of_type(column="flat", type_="INTEGER")
        # Проверка столбца на тип данных flat
        validator.expect_column_values_to_be_of_type(column="postcode", type_="INTEGER")
        # Проверка столбца на тип данных flat
        validator.expect_column_values_to_be_of_type(column="street", type_="VARCHAR")
        # Проверка на not null kato_id
        validator.expect_column_values_to_not_be_null(column="kato_id")
        # Проверка столбца на тип данных flat
        validator.expect_column_values_to_be_of_type(column="register_type", type_="VARCHAR")
        # Проверка на not null end_date
        validator.expect_column_values_to_not_be_null(column="end_date")
        # Проверка на not null bad_supplier_date
        validator.expect_column_values_to_not_be_null(column="bad_supplier_date")
        # Проверка на not null include_nomenclature_date
        validator.expect_column_values_to_not_be_null(column="include_nomenclature_date")
        # Проверка на not null create_date
        validator.expect_column_values_to_not_be_null(column="create_date")
        # Проверка на not null last_modified_date
        validator.expect_column_values_to_not_be_null(column="last_modified_date")
        # # Проверка длины id
        # validator.expect_column_value_lengths_to_equal(column="id", value=10)
# ----------------------------------------------------- #
print(validator.head())
validator_on = Validator()
validator_on.validation()

# Тесты
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)


# Вывод Allerting в браузер
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": "test_suite",
        }
    ],
}

checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_test_suite", context, **checkpoint_config
)
results = checkpoint.run()

context.build_data_docs()

validation_result_identifier = results.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)

# Получить список всех таблиц
# context.test_yaml_config(yaml.dump(datasource_config))

# Сохранить в конфиг
# context.add_datasource(**datasource_config)





























# import psycopg2
# import pandas as pd
# import sys
#
# # Connection parameters, yours will be different
# param_dic = {
#     "host"      : "3.124.15.169",
#     "database"  : "smedb",
#     "user"      : "sme",
#     "password"  : "thispasswordtoostrong"
# }
# def connect(params_dic):
#     """ Connect to the PostgreSQL database server """
#     conn = None
#     try:
#         # connect to the PostgreSQL server
#         print('Connecting to the PostgreSQL database...')
#         conn = psycopg2.connect(**params_dic)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#         sys.exit(1)
#     print("Connection successful")
#     return conn
#
#
# def postgresql_to_dataframe(conn, select_query, column_names):
#     """
#     Tranform a SELECT query into a pandas dataframe
#     """
#     cursor = conn.cursor()
#     try:
#         cursor.execute(select_query)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#         cursor.close()
#         return 1
#
#     # Naturally we get a list of tupples
#     tupples = cursor.fetchall()
#     cursor.close()
#
#     # We just need to turn it into a pandas dataframe
#     df = pd.DataFrame(tupples, columns=column_names)
#     return df
#
# # Connect to the database
# conn = connect(param_dic)
# column_names = ["id", "source", "datetime", "mean_temp", "id", "source", "datetime", "mean_temp", "id", "source", "datetime", "mean_temp", "id", "source", "datetime", "mean_temp", "id", "source", "datetime", "mean_temp", "id"]
# # Execute the "SELECT *" query
# df = postgresql_to_dataframe(conn, "select * from suppliers_stage LIMIT 10", column_names)
# df.head()
# df.to_csv(r'./pandas.txt', header=None, index=None, sep=' ', mode='a')