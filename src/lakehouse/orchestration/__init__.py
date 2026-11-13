"""Orchestration component: the Airflow-free logic behind ``dags/lakehouse_pipeline.py``.

Airflow triggers, Spark owns state (ADR-0010). This package builds commands, paths, gate
decisions and alert payloads as pure functions so they are unit-tested without Airflow, Docker or
Spark. It imports only ``lakehouse.contracts`` (ADR-0009 rule 5: orchestration reaches the other
components through their job interface - job name, arguments, exit code, JSON log - never through
imports).
"""
