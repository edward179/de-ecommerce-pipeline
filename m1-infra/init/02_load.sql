COPY olist.customers   FROM '/data/olist_customers_dataset.csv'      WITH (FORMAT csv, HEADER true);
COPY olist.sellers     FROM '/data/olist_sellers_dataset.csv'        WITH (FORMAT csv, HEADER true);
COPY olist.products    FROM '/data/olist_products_dataset.csv'       WITH (FORMAT csv, HEADER true);
COPY olist.product_category_name_translation
                       FROM '/data/product_category_name_translation.csv' WITH (FORMAT csv, HEADER true);
COPY olist.orders      FROM '/data/olist_orders_dataset.csv'         WITH (FORMAT csv, HEADER true);
COPY olist.order_items FROM '/data/olist_order_items_dataset.csv'    WITH (FORMAT csv, HEADER true);
COPY olist.order_payments FROM '/data/olist_order_payments_dataset.csv' WITH (FORMAT csv, HEADER true);
COPY olist.order_reviews  FROM '/data/olist_order_reviews_dataset.csv'  WITH (FORMAT csv, HEADER true);
