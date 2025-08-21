# BigQuery Anti-Pattern Recognition - Demo

Simple deployment and usage guide for the BigQuery Anti-Pattern Recognition tool.

## 🚀 Quick Start

This demo provides two interactive notebooks:

1. **[01_setup_and_deploy.ipynb](01_setup_and_deploy.ipynb)** - Complete deployment setup
2. **[02_streamlit_frontend.ipynb](02_streamlit_frontend.ipynb)** - Web interface for query analysis

## 📁 What's Included

```
demo/
├── 01_setup_and_deploy.ipynb      # Main deployment notebook
├── 02_streamlit_frontend.ipynb    # Streamlit web interface
├── cloudbuild-batch.yaml          # Batch processing build config
├── cloudbuild-service.yaml        # API service build config
├── config.json                    # Shared configuration
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## 🎯 Deployment Options

The tool can be deployed in three ways:

| Option | Use Case | Best For |
|--------|----------|----------|
| **Cloud Run Job** | Batch processing of query history | Scheduled audits, automated monitoring |
| **Cloud Run Service** | REST API for real-time analysis | CI/CD integration, interactive tools |
| **BigQuery UDF** | SQL-native analysis within BigQuery | Data analysts, ad-hoc analysis |

## 📋 Prerequisites

- Google Cloud Project with billing enabled
- `gcloud` CLI installed and authenticated
- Jupyter notebook environment

## 🔧 Getting Started

### Step 1: Run the Setup Notebook

```bash
jupyter notebook 01_setup_and_deploy.ipynb
```

This notebook will:
- Configure your Google Cloud project
- Enable required APIs
- Build and deploy containers
- Create BigQuery datasets and UDFs
- Test all deployments

### Step 2: Launch the Web Interface

```bash
jupyter notebook 02_streamlit_frontend.ipynb
```

This creates a Streamlit web app for:
- Interactive query analysis
- AI-powered query optimization
- Visual anti-pattern detection results

## 🔍 Anti-Pattern Detection

The tool detects common BigQuery anti-patterns:

- **SELECT \*** - Unnecessary column selection
- **ORDER BY without LIMIT** - Inefficient sorting
- **REGEXP_CONTAINS misuse** - When LIKE would be better
- **Inefficient WHERE ordering** - Poor condition placement
- **Multiple CTE evaluations** - Repeated computations
- **IN subqueries without aggregation** - Performance issues
- **Poor join ordering** - Suboptimal execution plans
- **Dynamic predicates** - Preventing optimizations
- **Inefficient latest record patterns** - Better alternatives exist
- **Missing DROP statements** - Resource cleanup issues

## 📚 Usage Examples

### Using the Streamlit Interface

1. Open the Streamlit notebook
2. Update `PROJECT_ID` and `SERVICE_URL` in the configuration
3. Run the notebook to generate and start the web app
4. Paste your SQL queries for analysis
5. Get instant feedback and AI-powered optimizations

### Using the BigQuery UDF

```sql
-- Analyze a single query
SELECT antipattern_demo.get_antipatterns(
  'SELECT * FROM dataset.table ORDER BY column'
) as analysis;

-- Analyze multiple queries
WITH queries AS (
  SELECT 'SELECT * FROM table1' as sql_query
  UNION ALL
  SELECT 'SELECT col1 FROM table2 ORDER BY col1' as sql_query
)
SELECT
  sql_query,
  antipattern_demo.get_antipatterns(sql_query) as analysis
FROM queries;
```

### Using the REST API

```bash
curl -X POST "YOUR_SERVICE_URL" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{"calls": [["SELECT * FROM dataset.table ORDER BY column"]]}'
```

## 🛠️ Troubleshooting

**Authentication Issues:**
```bash
gcloud auth login
gcloud auth application-default login
```

**Build Failures:**
- Ensure you're in the correct directory
- Verify Cloud Build API is enabled
- Check billing is enabled on your project

**Permission Issues:**
- Ensure your account has necessary IAM roles
- Verify service account permissions for BigQuery connections

## 🧹 Clean Up

To remove all deployed resources:

```bash
# Delete Cloud Run services
gcloud run jobs delete antipattern-batch-job --region=us-central1 --quiet
gcloud run services delete antipattern-api-service --region=us-central1 --quiet

# Delete BigQuery resources
bq query --use_legacy_sql=false "DROP FUNCTION antipattern_demo.get_antipatterns"
bq rm -r -d PROJECT_ID:antipattern_demo

# Delete container registry
gcloud artifacts repositories delete antipattern-registry --location=us-central1 --quiet
```

## 📖 Additional Resources

- [Main Documentation](../README.md) - Complete project documentation
- [Cloud Run Deployment Guide](../CR_DEPLOY.md) - Detailed deployment instructions
- [Terraform Module](../terraform/) - Infrastructure as Code

---

**🎉 Ready to get started? Run `jupyter notebook 01_setup_and_deploy.ipynb`**
