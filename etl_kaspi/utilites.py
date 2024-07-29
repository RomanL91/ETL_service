import httpx
import pandas as pd
import numpy as np

from core import settings


async def fetch_archive_orders_from_kaspi(data=None):
    headers = settings.etl_kaspi.headers.update({"X-Auth-Token": data.kaspi_token})

    async with httpx.AsyncClient() as client:
        try:
            # запрос у каспи
            response = await client.get(
                data.orders_api, headers=headers, params=data.params, timeout=30
            )
            response.raise_for_status()
            response_data = response.json()
            # получение вложенной инфы о вложенных продуктах и заявках
            included = response_data["included"]
            orders = response_data["data"]

            # ===== блок запросов к service_shop
            # pполучение от магазина ИД клиентов, ИД продуктов, ИД заявок которые уже в системе
            customer_id_into_sys = await client.get(
                settings.etl_kaspi.url_api_get_customers_id
            )
            prod_kaspi_id_into_sys = await client.get(
                settings.etl_kaspi.url_api_get_products_kaspi_id
            )
            orders_id_into_sys = await client.get(
                settings.etl_kaspi.url_api_get_orders_id
            )

            # ===== блок работы с инфой о вложенных продуктах
            # Загрузить данные в DataFrame
            orderentries_df = pd.DataFrame(included)
            # Расширение столбца 'attributes' и 'links' (может стоит подумать над записью проще)
            orderentries_attributes_df = (
                orderentries_df.drop(["attributes", "links"], axis=1)
                .join(
                    orderentries_df["attributes"]
                    .apply(pd.Series)
                    .join(
                        orderentries_df["attributes"]
                        .apply(lambda x: x["offer"])
                        .apply(pd.Series)
                    )
                )
                .drop(["relationships", "offer", "category"], axis=1)
            )
            # Фильтровать, чтобы сохранять только тех, которыйх нет в системе
            filtered_data_orderentries = orderentries_attributes_df[
                ~orderentries_attributes_df["id"].isin(prod_kaspi_id_into_sys.json())
            ]

            # ===== блок работы с инфой заявок/ордеров
            # Преобразование в DataFrame
            orders_df_full = pd.json_normalize(orders, sep="_")
            # заменяем все Nan на None
            orders_df_full.replace({np.nan: None}, inplace=True)
            # Выборка необходимых колонок для ордера
            orders_df = orders_df_full[
                [
                    "type",
                    "id",
                    "attributes_code",
                    "attributes_totalPrice",
                    "attributes_paymentMode",
                    "attributes_creationDate",
                    "attributes_deliveryCostForSeller",
                    "attributes_isKaspiDelivery",
                    "attributes_deliveryMode",
                    "attributes_deliveryAddress_formattedAddress",
                    "attributes_deliveryAddress_latitude",
                    "attributes_deliveryAddress_longitude",
                    "attributes_state",
                    "attributes_approvedByBankDate",
                    "attributes_status",
                    "attributes_customer_id",
                    "attributes_preOrder",
                    "relationships_entries_data",
                ]
            ]
            # Выборка необходимых колонок для пользователя
            customer_df = orders_df_full[
                [
                    "attributes_customer_id",
                    "attributes_customer_name",
                    "attributes_customer_cellPhone",
                    "attributes_customer_firstName",
                    "attributes_customer_lastName",
                ]
            ]
            # Фильтровать, чтобы сохранять только тех, которыйх нет в системе (ордеров и пользователей)
            filtered_data_orders = orders_df[
                ~orders_df["id"].isin(orders_id_into_sys.json())
            ]
            filtered_data_customer = customer_df[
                ~customer_df["attributes_customer_id"].isin(customer_id_into_sys.json())
            ]

            data = {
                "customers": filtered_data_customer.to_dict(orient="records"),
                "orderentries": filtered_data_orderentries.to_dict(orient="records"),
                "self_order": filtered_data_orders.to_dict(orient="records"),
            }

            return data
        except Exception as e:
            # возвращаю ошибку для просмотра в Flower
            return e
