CREATE TABLE tdb.branch (
    branch_id VARCHAR(10),
    branch_name VARCHAR(100)
);

CREATE TABLE tdb.team (
    team_id VARCHAR(10),
    team_name VARCHAR(100),
    branch_id VARCHAR(10)
);

CREATE TABLE tdb.agent (
    id VARCHAR(10),
    agent_code VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    team_id VARCHAR(10)
);

CREATE TABLE tdb.insurance_product (
    product_id VARCHAR(10),
    product_name VARCHAR(100),
    product_type VARCHAR(50),
    target INTEGER
);

CREATE TABLE tdb.agent_product (
    id VARCHAR(10),
    product_id VARCHAR(10)
);

CREATE TABLE tdb.sale (
    sale_id VARCHAR(10),
    sale_date DATE,
    id VARCHAR(10),
    product_id VARCHAR(10),
    amount INTEGER
);