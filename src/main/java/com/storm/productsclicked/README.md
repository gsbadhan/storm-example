# This sample shows user's product profiling across online web sites  

## Run ProductsClickedTopology.class
		

### install Apache cassandra and create below keyspace and table:

CREATE KEYSPACE IF NOT EXISTS products WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS products.user_product_profiling(
user_id text, 
site_url text, 
product text,
clicked counter,
PRIMARY KEY (user_id,site_url,product));

### start storm app and check in data in cassandra using below query:
##### select * from products.user_product_profiling limit 100;
##### select * from products.user_product_profiling where user_id='johan' and site_url='amazon.com';
##### select * from products.user_product_profiling where user_id='johan';



