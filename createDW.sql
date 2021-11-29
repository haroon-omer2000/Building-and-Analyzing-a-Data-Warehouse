DROP TABLE SALES;
DROP TABLE CUSTOMERS;
DROP TABLE PRODUCTS;
DROP TABLE STORES;
DROP TABLE TIME;
DROP TABLE SUPPLIERS;

CREATE TABLE CUSTOMERS(
	CUSTOMER_ID VARCHAR(4) NOT NULL,
	CUSTOMER_NAME VARCHAR(30) NOT NULL,
	CONSTRAINT CUSTOMER_PK PRIMARY KEY (CUSTOMER_ID)
);

CREATE TABLE PRODUCTS(
	PRODUCT_ID VARCHAR(6) NOT NULL,
	PRODUCT_NAME VARCHAR(30) NOT NULL,
	CONSTRAINT PRODUCT_PK PRIMARY KEY (PRODUCT_ID)
);

CREATE TABLE SUPPLIERS(
	SUPPLIER_ID VARCHAR(5) NOT NULL,
	SUPPLIER_NAME VARCHAR(30) NOT NULL,
	CONSTRAINT SUPPLIER_PK PRIMARY KEY (SUPPLIER_ID)
);

CREATE TABLE STORES(
	STORE_ID VARCHAR(4) NOT NULL,
	STORE_NAME VARCHAR(20) NOT NULL,
	CONSTRAINT STORE_PK PRIMARY KEY (STORE_ID)
);

CREATE TABLE TIME(
TIME_ID DATE NOT NULL,
DAY INT NOT NULL,
MONTH INT NOT NULL,
QUARTER INT NOT NULL,
YEAR INT NOT NULL,
CONSTRAINT TIME_PK PRIMARY KEY (TIME_ID)
);

CREATE TABLE SALES(
	CUSTOMER_ID VARCHAR(4) NOT NULL ,
	SUPPLIER_ID VARCHAR(5) NOT NULL ,
	PRODUCT_ID VARCHAR(6) NOT NULL ,
	TIME_ID DATE NOT NULL ,
	STORE_ID VARCHAR(4) NOT NULL ,
	QUANTITY INT,
	TOTAL_SALE FLOAT,
	FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID),
	FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIERS(SUPPLIER_ID),
	FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID),
	FOREIGN KEY (TIME_ID) REFERENCES TIME(TIME_ID),
	FOREIGN KEY (STORE_ID) REFERENCES STORES(STORE_ID)
);
