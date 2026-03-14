-- MemoryGuard Demo — Seed Data
-- Creates realistic tables for anomaly detection testing.

CREATE TABLE IF NOT EXISTS departments (
    id          SERIAL PRIMARY KEY,
    dept_name   VARCHAR(100) NOT NULL,
    budget      NUMERIC(12, 2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS employees (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    email       VARCHAR(200),
    dept_id     INTEGER REFERENCES departments(id),
    salary      NUMERIC(10, 2),
    ssn         VARCHAR(11),          -- sensitive!
    bank_account VARCHAR(30),         -- sensitive!
    hire_date   DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS payroll (
    id          SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id),
    pay_date    DATE NOT NULL,
    gross_pay   NUMERIC(10, 2),
    net_pay     NUMERIC(10, 2),
    tax         NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL PRIMARY KEY,
    user_name   VARCHAR(100),
    action      VARCHAR(50),
    detail      TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Seed departments
INSERT INTO departments (dept_name, budget) VALUES
    ('Engineering', 2000000),
    ('Human Resources', 500000),
    ('Finance', 1500000),
    ('Marketing', 800000);

-- Seed employees
INSERT INTO employees (name, email, dept_id, salary, ssn, bank_account) VALUES
    ('Alice Johnson',  'alice@example.com',   1, 120000, '123-45-6789', 'ACCT-001'),
    ('Bob Smith',      'bob@example.com',     1, 115000, '234-56-7890', 'ACCT-002'),
    ('Carol Williams', 'carol@example.com',   2,  95000, '345-67-8901', 'ACCT-003'),
    ('Dave Brown',     'dave@example.com',    3, 130000, '456-78-9012', 'ACCT-004'),
    ('Eve Davis',      'eve@example.com',     4,  85000, '567-89-0123', 'ACCT-005');

-- Seed payroll
INSERT INTO payroll (employee_id, pay_date, gross_pay, net_pay, tax) VALUES
    (1, '2025-06-01', 10000, 7500, 2500),
    (2, '2025-06-01',  9583, 7187, 2396),
    (3, '2025-06-01',  7917, 5938, 1979),
    (4, '2025-06-01', 10833, 8125, 2708),
    (5, '2025-06-01',  7083, 5312, 1771);
