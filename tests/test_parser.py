"""Tests for src.parser.sql_fingerprint."""

import pytest

from src.parser.sql_fingerprint import SQLFingerprint, parse_sql


class TestParseSQL:
    """SQL → fingerprint extraction."""

    def test_simple_select(self):
        fp = parse_sql("SELECT id, name FROM employees WHERE id = 1")
        assert fp.operation == "SELECT"
        assert "employees" in fp.tables
        assert "id" in fp.columns
        assert "name" in fp.columns

    def test_insert(self):
        fp = parse_sql("INSERT INTO orders (customer_id, total) VALUES (1, 99.99)")
        assert fp.operation == "INSERT"
        assert "orders" in fp.tables
        assert "customer_id" in fp.columns
        assert "total" in fp.columns

    def test_update(self):
        fp = parse_sql("UPDATE employees SET salary = 50000 WHERE id = 42")
        assert fp.operation == "UPDATE"
        assert "employees" in fp.tables
        assert "salary" in fp.columns
        assert "id" in fp.columns

    def test_delete(self):
        fp = parse_sql("DELETE FROM audit_log WHERE created_at < '2024-01-01'")
        assert fp.operation == "DELETE"
        assert "audit_log" in fp.tables
        assert "created_at" in fp.columns

    def test_join_captures_all_tables(self):
        sql = (
            "SELECT e.name, d.dept_name "
            "FROM employees e JOIN departments d ON e.dept_id = d.id"
        )
        fp = parse_sql(sql)
        assert fp.operation == "SELECT"
        assert "employees" in fp.tables
        assert "departments" in fp.tables
        assert "name" in fp.columns
        assert "dept_name" in fp.columns

    def test_subquery_columns(self):
        sql = (
            "SELECT name FROM employees "
            "WHERE dept_id IN (SELECT id FROM departments WHERE budget > 1000)"
        )
        fp = parse_sql(sql)
        assert "name" in fp.columns
        assert "dept_id" in fp.columns
        assert "id" in fp.columns
        assert "budget" in fp.columns

    def test_fingerprint_hash_deterministic(self):
        fp1 = parse_sql("SELECT id, name FROM employees")
        fp2 = parse_sql("SELECT name, id FROM employees")
        assert fp1.fingerprint_hash == fp2.fingerprint_hash

    def test_malformed_sql_raises(self):
        with pytest.raises(ValueError):
            parse_sql("")

    def test_columns_normalised_lowercase(self):
        fp = parse_sql('SELECT "Name", "AGE" FROM employees')
        # SQLGlot lowercases identifiers by default
        assert all(c == c.lower() for c in fp.columns)
