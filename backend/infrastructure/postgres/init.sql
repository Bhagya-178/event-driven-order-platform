CREATE DATABASE orders_db;
CREATE USER order_user WITH ENCRYPTED PASSWORD 'order_pass';
GRANT ALL PRIVILEGES ON DATABASE orders_db TO order_user;
\c orders_db
GRANT ALL ON SCHEMA public TO order_user;
CREATE DATABASE orders_db;
CREATE USER order_user WITH ENCRYPTED PASSWORD 'order_pass';
GRANT ALL PRIVILEGES ON DATABASE orders_db TO order_user;
\c orders_db
GRANT ALL ON SCHEMA public TO order_user;
