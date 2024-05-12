--Script written for Postgres sql in pgAdmin 4

--Load orders data.
CREATE TABLE IF not EXISTS public.orders
(
    order_id character varying(255) NOT NULL,
    customer_id character varying(255) NOT NULL,
    order_status character varying(100),
    order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp,
    PRIMARY KEY (order_id)
);

COPY public.orders
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER;

--Load customers data.
CREATE TABLE IF not EXISTS public.customers
(
    customer_id character varying(255) NOT NULL,
    customer_unique_id character varying(255) NOT NULL,
    customer_zip_code_prefix bigint,
    customer_city character varying(255),
    customer_state character(2),
    PRIMARY KEY (customer_id)
);

COPY public.customers
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;


--Load products data.
CREATE TABLE IF not EXISTS public.products
(
    product_id character varying(255) NOT NULL,
    product_category character varying(150),
    product_name_length integer,
	product_description_length integer,
	product_photos_qty integer,
	product_weight_g bigint,
	product_length_cm integer,
	product_height_cm integer,
	product_width_cm integer,
    PRIMARY KEY (product_id)
);

COPY public.products
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_products_dataset.csv'
DELIMITER ','
CSV HEADER;

--create order_payments

CREATE TABLE public.order_payments (
    order_id character varying(50) not null,
    payment_sequential INTEGER,
    payment_type character varying(20),
    payment_installments INTEGER,
    payment_value NUMERIC(10, 2)
);

COPY public.order_payments
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

select * from public.order_payments;

--create order_items
CREATE TABLE public.order_items (
    order_id character varying(50) not null,
    order_item_id INTEGER,
    product_id character varying(50),
	seller_id character varying(50),
    shipping_limit_date timestamp,
	price numeric(10,2),
	freight_value numeric(10,2)
);


COPY public.order_items
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER;


select products.product_category, avg(items.price + items.freight_value) as Avg_Total_Price
from public.order_items as items
join public.products as products
on items.product_id = products.product_id
where products.product_category is not null
group by products.product_category
order by Avg_Total_Price desc
limit(10);

--create order_reviews table
CREATE TABLE public.order_reviews (
    review_id character varying(50) not null,
    order_id character varying(50) not null,
    review_score integer,
    review_comment_title character varying,
	review_comment_message character varying,
	review_creation_date timestamp,
	review_answer_timestamp timestamp
);


COPY public.order_reviews
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;




--create seller table
CREATE TABLE public.sellers (
    seller_id VARCHAR(50) not null,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);


COPY public.sellers
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

select * from public.sellers;

--create translation table
CREATE TABLE public.product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

COPY public.product_category_name_translation
FROM 'D:\Data_Engineering_Projects\Olist_dataset\data\product_category_name_translation.csv'
DELIMITER ','
CSV HEADER;

select * from product_category_name_translation;

--Total number of reviews with english translations
select pcnt.product_category_name_english as category, count(reviews.review_score) as total_reviews from public.order_reviews as reviews
join public.order_items as items
on items.order_id = reviews.order_id
join public.products as products
on products.product_id = items.product_id
join public.product_category_name_translation as pcnt
on pcnt.product_category_name = products.product_category
where pcnt.product_category_name_english is not null
group by pcnt.product_category_name_english
order by total_reviews desc limit(10);