# lakeFS Iceberg REST Catalog -> AWS Glue Catalog Federation

Query lakeFS-managed Apache Iceberg tables from **Amazon Athena** using **AWS Glue Catalog Federation** -- no data copying, no metadata sync, real-time access.

## How it works

```mermaid
flowchart LR
    subgraph AWS
        Athena["Athena / Redshift / EMR"]
        Glue["Glue Data Catalog (Federated)"]
        LF["Lake Formation"]
        S3["Amazon S3 (Iceberg data & metadata)"]
    end

    subgraph External
        lakeFS["lakeFS\nIceberg REST Catalog"]
    end

    Athena -- "SQL query" --> Glue
    Glue -- "Iceberg REST API (OAuth2)" --> lakeFS
    lakeFS -. "metadata-location (s3://... paths)" .-> Glue
    LF -- "scoped S3\ncredentials" --> Athena
    Athena -- "read data files" --> S3
    lakeFS -. "data files live here" .-> S3
```

1. **Glue** connects to the lakeFS Iceberg REST Catalog via OAuth2 and calls standard Iceberg REST API endpoints (`listNamespaces`, `listTables`, `loadTable`)
2. **lakeFS** returns table metadata including physical S3 paths to `metadata.json`, manifest lists, and data files
3. **Lake Formation** vends temporary, scoped S3 credentials to the query engine
4. **Athena** (or Redshift/EMR) reads Iceberg data files directly from S3

The external catalog is only used for **metadata discovery**. All data access goes through S3 with Lake Formation-managed credentials.

## Quick start

### Prerequisites

- Python 3.11+
- AWS credentials configured (e.g. `~/.aws/credentials`, environment variables, or IAM role) with permissions for IAM, Glue, Lake Formation, and Secrets Manager
- A lakeFS instance with the Iceberg REST Catalog enabled
- A lakeFS service account (access key + secret key)

### Install and run

Using [uv](https://docs.astral.sh/uv/) (no install needed):

```bash
uvx lakefs-glue-federation \
    --lakefs-url https://my-org.us-east-1.lakefscloud.io \
    --lakefs-repo my-repo \
    --lakefs-ref main \
    --lakefs-access-key-id AKIAIOSFODNN7EXAMPLE \
    --lakefs-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
    --grant-to arn:aws:iam::123456789012:role/DataAnalysts
```

Using pip:

```bash
pip install lakefs-glue-federation

lakefs-glue-federation \
    --lakefs-url https://my-org.us-east-1.lakefscloud.io \
    --lakefs-repo my-repo \
    --lakefs-ref main \
    --lakefs-access-key-id AKIAIOSFODNN7EXAMPLE \
    --lakefs-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
    --grant-to arn:aws:iam::123456789012:role/DataAnalysts
```

From source:

```bash
git clone https://github.com/treeverse/lakefs-glue-federation.git
cd lakefs-glue-federation

# Using uv
uv sync
uv run lakefs-glue-federation --help

# Or using pip
pip install -e .
lakefs-glue-federation --help
```

Query from Athena:

```sql
SELECT * FROM "lakefs-catalog"."default"."my_table" LIMIT 10;
```

### Options

| Option | Required | Default | Description |
|---|---|---|---|
| `--lakefs-url` | Yes | - | lakeFS server URL |
| `--lakefs-repo` | Yes | - | lakeFS repository name |
| `--lakefs-ref` | No | `main` | lakeFS ref to expose (branch, tag, or commit ID) |
| `--lakefs-access-key-id` | Yes | - | lakeFS access key ID |
| `--lakefs-secret-access-key` | Yes | - | lakeFS secret access key |
| `--catalog-name` | No | `lakefs-catalog` | Name for the Glue federated catalog |
| `--region` | No | `us-east-1` | AWS region |
| `--grant-to` | No | - | IAM ARNs to grant catalog access (repeatable) |

### Multiple branches, tags, and repositories

Each federated catalog is scoped to a single lakeFS **repository + ref** (branch, tag, or commit ID). To expose multiple refs:

```bash
# Main branch
uvx lakefs-glue-federation \
    --lakefs-url https://my-org.lakefscloud.io \
    --lakefs-repo my-repo --lakefs-ref main \
    --catalog-name my-repo-main \
    --lakefs-access-key-id ... --lakefs-secret-access-key ...

# Dev branch
uvx lakefs-glue-federation \
    --lakefs-url https://my-org.lakefscloud.io \
    --lakefs-repo my-repo --lakefs-ref dev \
    --catalog-name my-repo-dev \
    --lakefs-access-key-id ... --lakefs-secret-access-key ...

# A tagged release (point-in-time snapshot)
uvx lakefs-glue-federation \
    --lakefs-url https://my-org.lakefscloud.io \
    --lakefs-repo my-repo --lakefs-ref v1.0 \
    --catalog-name my-repo-v1 \
    --lakefs-access-key-id ... --lakefs-secret-access-key ...
```

All catalogs appear independently in Athena and Lake Formation. This lets you query a stable tagged snapshot alongside the latest branch data, or compare across branches by joining different catalogs.

## What the script creates

| AWS Resource | Name Pattern | Purpose |
|---|---|---|
| Secrets Manager secret | `{catalog-name}-secret` | Stores lakeFS secret key for OAuth2 |
| IAM role | `{catalog-name}-GlueConnectionRole` | Assumed by Glue and Lake Formation |
| Glue Connection | `{catalog-name}-connection` | REST API bridge to lakeFS |
| Lake Formation resource | (registered connection) | Enables S3 credential vending |
| Glue Catalog | `{catalog-name}` | The federated catalog visible in Athena |
| Lake Formation grants | (on the catalog) | Permissions for specified principals |

The script is **idempotent** -- rerunning with the same parameters updates resources in place. Rerunning with changed parameters (e.g., different branch or credentials) converges to the new state.

## Limitations

- **Read-only**: Glue Catalog Federation only supports queries ([AWS docs](https://docs.aws.amazon.com/glue/latest/dg/limitation-glue-iceberg-rest-api.html)). You cannot `INSERT INTO`, `CREATE TABLE`, or modify data through the federated catalog. Use Spark/PyIceberg/Trino connected directly to lakeFS for writes.
- **Single ref per catalog**: Each federated catalog points to one lakeFS ref (branch or tag). Create multiple catalogs to expose multiple refs.
- **No nested namespaces**: Glue catalog federation only supports single-level namespaces ([AWS docs](https://docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation-snowflake.html)). Tables must follow a flat `catalog.namespace.table` structure. This is why each catalog must be scoped to a specific `repo.ref` - it flattens the lakeFS hierarchy so namespaces within the ref are exposed as top-level databases.

## AWS SDK/CLI configuration notes

The AWS Console handles these automatically, but when automating via SDK/CLI they need to be set explicitly:

1. **Single IAM role for Glue + Lake Formation**: The same IAM role must be used for both the Glue connection's `ROLE_ARN` and the Lake Formation `RegisterResource` call. Using separate roles (even with identical policies) causes federation to silently fail - `get_databases` times out with `FederationSourceRetryableException` and zero HTTP requests reach the external catalog.

2. **`SUPER_USER` Lake Formation grant**: The `SUPER_USER` permission must be granted on the federated catalog for it to appear in the Lake Formation console UI. Without it, the catalog works via CLI/SDK but is invisible in the console.

3. **`WithPrivilegedAccess` in `RegisterResource`**: The `RegisterResource` call should include `WithPrivilegedAccess=True` to grant the registering principal full control over the federated resource.

## Technical details

See the annotated source in [`lakefs_glue_federation.py`](lakefs_glue_federation.py) for a detailed walkthrough of the integration architecture, OAuth2 authentication flow, namespace mapping, and S3 credential vending.
