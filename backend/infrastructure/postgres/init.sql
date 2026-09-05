-- Order Service Database and User
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'order_user') THEN
      CREATE USER order_user WITH ENCRYPTED PASSWORD 'order_pass';
   ELSE
      ALTER USER order_user WITH ENCRYPTED PASSWORD 'order_pass';
   END IF;
END
$$;

SELECT 'CREATE DATABASE orders_db OWNER order_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'orders_db')\gexec

GRANT ALL PRIVILEGES ON DATABASE orders_db TO order_user;
\c orders_db
GRANT ALL ON SCHEMA public TO order_user;

-- Payment Service Database and User
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'payment_user') THEN
      CREATE USER payment_user WITH ENCRYPTED PASSWORD 'payment_pass';
   ELSE
      ALTER USER payment_user WITH ENCRYPTED PASSWORD 'payment_pass';
   END IF;
END
$$;

SELECT 'CREATE DATABASE payments_db OWNER payment_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'payments_db')\gexec

GRANT ALL PRIVILEGES ON DATABASE payments_db TO payment_user;
\c payments_db
GRANT ALL ON SCHEMA public TO payment_user;

-- Inventory Service Database and User
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'inventory_user') THEN
      CREATE USER inventory_user WITH ENCRYPTED PASSWORD 'inventory_pass';
   ELSE
      ALTER USER inventory_user WITH ENCRYPTED PASSWORD 'inventory_pass';
   END IF;
END
$$;

SELECT 'CREATE DATABASE inventory_db OWNER inventory_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventory_db')\gexec

GRANT ALL PRIVILEGES ON DATABASE inventory_db TO inventory_user;
\c inventory_db
GRANT ALL ON SCHEMA public TO inventory_user;


