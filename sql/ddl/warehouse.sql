CREATE TABLE dwh.dim_date (
    date_sk INTEGER,
    date DATE
);

CREATE TABLE dwh.dim_product (
    product_sk INTEGER,
    product_id VARCHAR(10),
    product_name VARCHAR(100),
    product_type VARCHAR(50),
    target INTEGER
);

CREATE TABLE dwh.dim_agent (
    agent_sk INTEGER,
    id VARCHAR(10),
    agent_name VARCHAR(200),
    team_name VARCHAR(100),
    branch_name VARCHAR(100)
);

CREATE TABLE dwh.fact_sales (
    sale_id VARCHAR(10),
    date_sk INTEGER,
    product_sk INTEGER,
    agent_sk INTEGER,
    amount INTEGER
);

CREATE TABLE dwh.sales_team_aggr (
    team_name VARCHAR(100),
    total_sale_amount INTEGER
);

CREATE TABLE dwh.sales_branch_aggr (
    branch_name VARCHAR(100),
    total_sale_amount INTEGER
);

CREATE TABLE dwh.sales_product_aggr (
    product_name VARCHAR(100),
    total_sale_amount INTEGER,
    target INTEGER,
    sales_target_achieved VARCHAR(3)
);