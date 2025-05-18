## Project Structure

├── models/ │ ├── staging/ # Initial data cleaning and validation │ ├── intermediate/ # Business logic implementation │ └── marts/ # Final analysis views ├── analysis/ │ └── visualizations/ # Python visualization scripts └── tests/ # Custom dbt tests


### Prerequisites
- dbt
- DuckDB
- Python (for visualizations)
- Required Python packages: pandas, seaborn, matplotlib

### Setup
1. Clone this repository
2. Configure DuckDB profile (see Appendix in documentation)
3. Install required packages:
```bash
pip install duckdb pandas seaborn matplotlib
DBT Commands Reference

Basic Commands

# Run all models
dbt run

# Run specific models
dbt run --select staging
dbt run --select marts
dbt run --select model_name

# Test all models
dbt test

# Run specific tests
dbt test --select test_type:generic
dbt test --select test_type:singular
dbt test --select test_name:valid_id_range

# Run tests with failure storage
dbt test --store-failures

# Run models and tests together
dbt build
```

### Visualization

To generate analysis plots:

python analysis/visualizations/time_analysis_viz.py