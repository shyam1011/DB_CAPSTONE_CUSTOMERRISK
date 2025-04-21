Databricks Data Engineering & Architecture Candidate Assessment
1. Real-World Use Case: Customer 360 for a FinTech Lending Platform
A growing fintech company wants to build a Customer 360 Data Lakehouse platform on Databricks (on Azure or AWS), which will serve analytics, reporting, and predictive modeling; near real-time credit risk evaluation; and open data sharing with partner banks via Delta Sharing. The platform ingests data from various sources including Loan Applications, Customer Profiles, Transactions, and Credit Scores using Fivetran, DBT, Kafka, and APIs. The goal is to build an end-to-end data platform leveraging Databricks Lakehouse capabilities.
2. Solution Architecture Expectations
•	Data Ingestion: Streaming via Kafka & Autoloader, Batch via Fivetran/DBT/APIs, Incremental loads.
•	Data Lakehouse Modeling: Delta Lake (or Iceberg), ZORDER, Liquid Clustering, Deletion Vectors, Predictive Optimization.
•	Processing Layers: Bronze → Silver → Gold using Delta Live Tables with Expectations.
•	Data Serving: SQL Warehouse, Streaming Tables, Materialized Views, TVFs, AI functions.
•	Jobs & Workflows: Workflows with Tasks, Failure Handling, Alerting.
•	DevOps & Infra: Terraform provisioning, CI/CD, Databricks Asset Bundles.
•	ML Engineering: MLFlow, Feature Store, Model Versioning.
•	Observability & Cost: Monitoring, Alerting, Cost attribution, LakeView dashboards.
•	Data Sharing: Delta Sharing, Unity Catalog access control.
•	GenAI Exposure (Bonus): Chatbot using LLMs over customer risk data.
3. Coding & Design Exercise
Split into three parts to evaluate both hands-on and architectural capabilities.
Part 1: Architecture Design 
Submit a detailed architecture diagram with explanation of ingestion, storage, processing, serving, sharing, cost and security considerations.
Part 2: Data Pipeline Exercise 
Implement a Bronze → Silver → Gold pipeline for Customer + Transaction data. Use Autoloader, DLT, Delta Optimizations, and deploy using Databricks Asset Bundle with Unity Catalog and Workflow.

Part 3: SQLWarehouse + ML
Write SQL queries to demonstrate Streaming Tables, TVFs, MVs, and optionally build a credit scoring model with MLFlow.
4. Deliverables
•	GitHub repo with Terraform, DLT pipelines, SQL queries, Asset Bundle.
•	Architecture diagram and ReadMe.
•	Optional: short video walkthrough (~5-10 mins).
5. Evaluation Criteria
Each candidate will be scored on the following areas:
Area	Weight	Evaluation Points
Delta Lake & Optimization	15%	ZORDER, Liquid Clustering, Predictive Optim, Deletes
Streaming & DLT	15%	Incremental pipelines, Autoloader, DLT Expectations
SQL & Data Modeling	10%	SQLWarehouse, TVFs, MVs, AI functions
Unity Catalog	10%	Catalog setup, access control, Governance, sharing
Infra / DevOps	10%	Terraform, Bundles, CI/CD
MLFlow & ML Pipeline	10%	Feature Table, tracking, scoring
Observability & Cost	10%	Monitoring, alerting, cluster config
Architecture Clarity	10%	End-to-end design, modularity, reusability
Communication & Docs	10%	ReadMe quality, clarity of explanations

