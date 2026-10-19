-- M7: read-only login for the event producer (samples real customer and product IDs).
-- Run once as the M1 superuser. The password is passed with psql -v, never stored here.
CREATE ROLE event_producer LOGIN PASSWORD :'producer_pw';
GRANT CONNECT ON DATABASE olist TO event_producer;
GRANT USAGE ON SCHEMA olist TO event_producer;
GRANT SELECT ON olist.customers, olist.order_items TO event_producer;
