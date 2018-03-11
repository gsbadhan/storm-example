# This sample shows how to track stock price on intervals  

## Run ProductsClickedTopology.class
		

### cassandra database keyspace and table:

CREATE KEYSPACE IF NOT EXISTS products WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS products.user_product_profiling(
user_id text, 
site_url text, 
product text,
clicked counter,
PRIMARY KEY (user_id,site_url,product));



