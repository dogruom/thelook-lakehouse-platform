"""Multi-tenant CRM lakehouse platform.

One installable package with independent components (ADR-0009). Components interact only
through `lakehouse.contracts`, Kafka topics, storage paths and tables.
"""

__version__ = "0.1.0"
