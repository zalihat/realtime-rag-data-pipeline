# realtime-rag-data-pipeline
realtime-rag-data-pipeline/

├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── infra/
│   ├── postgres/
│   │   └── init.sql                    # schema + REPLICA IDENTITY + publication
│   └── debezium/
│       └── register-connector.json
│
├── streaming/                          # CDC → RAG path (your deep leg)
│   ├── consumer/
│   │   ├── consumer.py                 # Kafka consumer -> chunk -> embed -> upsert
│   │   ├── chunking.py
│   │   ├── embeddings.py
│   │   └── vector_store.py             # pgvector upsert/delete logic
│   ├── requirements.txt
│   └── Dockerfile
│
├── batch/                              # Batch → warehouse path
│   ├── dags/
│   │   └── warehouse_load_dag.py       # Airflow DAG
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   └── marts/
│   │   │       ├── fct_orders.sql
│   │   │       ├── dim_customer.sql
│   │   │       └── dim_product.sql
│   │   └── dbt_project.yml
│   └── README.md
│
├── feature_store/                      # Feast, thin leg
│   ├── feature_repo/
│   │   ├── features.py                 # feature definitions (dbt-backed)
│   │   └── feature_store.yaml
│   ├── materialize.py                  # nightly job -> Redis online store
│   └── point_in_time_demo.py           # the correctness-proof query
│
├── demo/                               # Week 3 deliverable
│   ├── app.py                          # Streamlit demo UI
│   ├── scenario_script.py              # simulates Sarah -> Mike end to end
│   └── assets/
│       └── demo.gif
│
├── docs/
│   ├── architecture.png
│   ├── architecture.md                 # the "why" writeup
│   └── metrics.md                      # latency numbers, tradeoffs
│
└── tests/
    ├── test_consumer.py
    └── test_feature_correctness.py