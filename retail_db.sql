/*
Obtained from Cloudera Quick Start VM - CDH 5.5.0.
We are using PostgresSQL 15 on a  Red Hat family distribution (Fedora 38)
refer to pg_installation.sh for installation 
*/

-- create database as retail_db with public schema

CREATE DATABASE IF NOT EXISTS retail_db;

-- table departments

CREATE TABLE departments (
  department_id INT NOT NULL PRIMARY KEY,
  department_name VARCHAR(55) NOT NULL
);

-- table categories

CREATE TABLE categories (
  category_id INT NOT NULL PRIMARY KEY ,
  category_department_id INT NOT NULL,
  category_name VARCHAR(55) NOT NULL,
  FOREIGN KEY (category_department_id)
  REFERENCES departments (department_id)
);

-- table products

CREATE TABLE products (
  product_id INT NOT NULL PRIMARY KEY,
  product_category_id INT NOT NULL,
  product_name VARCHAR(55) NOT NULL,
  product_description VARCHAR(255) NOT NULL,
  product_price FLOAT NOT NULL,
  product_image VARCHAR(255) NOT NULL,
  FOREIGN KEY (product_category_id)
  REFERENCES categories (category_id)  
);

-- table customers

CREATE TABLE customers (
  customer_id INT NOT NULL PRIMARY KEY,
  customer_fname VARCHAR(55) NOT NULL,
  customer_lname VARCHAR(55) NOT NULL,
  customer_email VARCHAR(55) NOT NULL,
  customer_password VARCHAR(55) NOT NULL,
  customer_street VARCHAR(255) NOT NULL,
  customer_city VARCHAR(55) NOT NULL,
  customer_state VARCHAR(55) NOT NULL,
  customer_zipcode VARCHAR(55) NOT NULL
);

-- table orders

CREATE TABLE orders (
  order_id INT NOT NULL   PRIMARY KEY ,
  order_date TIMESTAMP NOT NULL,
  order_customer_id INT NOT NULL,
  order_status VARCHAR(55) NOT NULL,
);

-- table order_items

CREATE TABLE order_items (
  order_item_id INT NOT NULL PRIMARY KEY,
  order_item_order_id INT NOT NULL,
  order_item_product_id INT NOT NULL,
  order_item_quantity INT NOT NULL,
  order_item_subtotal FLOAT NOT NULL,
  order_item_product_price FLOAT NOT NULL
);

