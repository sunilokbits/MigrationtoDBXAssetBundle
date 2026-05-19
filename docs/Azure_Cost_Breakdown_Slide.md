Title: Azure Cost Breakdown for SQL Server to Databricks Migration Studio (100 GB)

---

**Reference Architecture Components:**
- Azure SQL Database / SQL Server (source)
- Azure Databricks (Unity Catalog, ETL, Notebooks)
- Azure Data Lake Storage (Bronze/Silver/Gold layers)
- Azure App Service (Flask Migration Studio)
- Azure Key Vault (Secrets/Keys)
- Azure Storage (Logs, Metadata)
- Azure Monitor/Log Analytics
- Azure Active Directory
- Azure Virtual Network

---

**Estimated Monthly Cost Breakdown (100 GB Data):**

| Component                | SKU/Size Example                | Est. Monthly Cost (USD) | Notes |
|--------------------------|---------------------------------|-------------------------|-------|
| Azure SQL Database       | Standard S2 (250 GB)            | $75                     | Source/metadata |
| Azure Databricks         | 1 DS3 v2 cluster (light use)    | $200–$400               | Main cost driver |
| Data Lake Storage        | 100 GB, Hot, LRS                | $2–$3                   | Data layers |
| App Service (Flask)      | Basic B1                        | $15                     | Web/API hosting |
| Key Vault                | Standard                        | $1–$2                   | Secrets mgmt |
| Azure Storage (logs)     | 50 GB, Hot, LRS                 | $1                      | Logs/files |
| Monitor/Log Analytics    | 5 GB ingested                   | $10                     | Monitoring |
| Virtual Network          | Basic                           | $0                      | Included |
| Active Directory         | Basic                           | $0                      | Included |

**Total Estimated Monthly Cost:** $300 – $500 USD

---

**Notes:**
- Databricks is the largest cost driver; costs scale with cluster size and usage.
- Storage costs are low for 100 GB unless high transaction rates.
- App Service and SQL DB can be scaled up/down as needed.
- Monitoring, Key Vault, and networking are minor costs at this scale.

---

For detailed pricing, use the Azure Pricing Calculator: https://azure.microsoft.com/en-us/pricing/calculator/
