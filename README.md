## ELT pipeline from Synthea csv data to OMOP CDM with Airflow, dbt Core, Cosmos

Welcome! illustrating a realistic data pipeline implemented from extracting electronic medical records (EMR), various medical vocabularies, loading onto a database and conducting transformation using dbt, while the workflow is orchestrated, scheduled and monitored using Apache airflow.

> The pipeline in this repo is described in more detail [easydata - Portfolio project](https://kurt1984.github.io/easydata/)

Key tools used:

- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).
- [dbt Core](https://docs.getdbt.com/docs/introduction).
- [Cosmos](https://github.com/astronomer/astronomer-cosmos).