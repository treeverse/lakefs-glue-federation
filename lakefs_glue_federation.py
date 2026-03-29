#!/usr/bin/env python3
"""
Set up AWS Glue Catalog Federation for a lakeFS Iceberg REST Catalog.

Creates all required AWS resources to query lakeFS-managed Iceberg tables
from Amazon Athena via Glue Catalog Federation.

Architecture:
    Athena --> Glue Data Catalog (federated) --> lakeFS Iceberg REST Catalog --> S3
                                                       |
                                              OAuth2 authentication
                                                       |
                                              Lake Formation vends
                                              scoped S3 credentials

The script creates these AWS resources:
    1. Secrets Manager secret    - stores lakeFS credentials for OAuth2
    2. IAM role                  - assumed by both Glue (for catalog access) and
                                   Lake Formation (for S3 credential vending)
    3. Glue Connection           - ICEBERGRESTCATALOG type, connects to lakeFS REST API
    4. Lake Formation resource   - registers the connection for federation
    5. Glue Federated Catalog    - the catalog visible in Athena/Lake Formation
    6. Lake Formation grants     - permissions so users can see/query the catalog

Idempotent -- safe to rerun with the same or updated parameters.
All resources are created or updated in place.

Important implementation notes (discovered through testing, March 2026):
    - RegisterResource must use the SAME IAM role as the Glue connection.
      Using a separate Lake Formation role causes federation to silently fail.
    - SUPER_USER permission must be granted on the catalog for it to appear
      in the Lake Formation console UI.

Usage:
    uv run setup_federation.py \\
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \\
        --lakefs-repo my-repo \\
        --lakefs-access-key-id AKIAIOSFODNN7EXAMPLE \\
        --lakefs-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \\
        --grant-to arn:aws:iam::123456789012:role/DataAnalysts
"""

import json
import time
from urllib.parse import urlparse

import boto3
import click
import lakefs_sdk
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Helper: AWS account ID
# ---------------------------------------------------------------------------

def get_account_id(sts):
    return sts.get_caller_identity()['Account']


# ---------------------------------------------------------------------------
# Helper: Resolve the S3 bucket from a lakeFS repository
# ---------------------------------------------------------------------------

def get_storage_bucket(lakefs_url, access_key_id, secret_access_key, repo_name):
    """Query the lakeFS API to discover which S3 bucket backs a repository.

    Every lakeFS repository has a "storage namespace" - an S3 prefix where
    Iceberg metadata and data files are physically stored. We need the bucket
    name to grant the IAM role read access to it.
    """
    config = lakefs_sdk.Configuration(
        host=f'{lakefs_url}/api/v1',
        username=access_key_id,
        password=secret_access_key,
    )
    client = lakefs_sdk.ApiClient(config)
    api = lakefs_sdk.RepositoriesApi(client)
    repo = api.get_repository(repo_name)
    parsed = urlparse(repo.storage_namespace)
    bucket = parsed.netloc
    click.echo(f"  Storage namespace: {repo.storage_namespace}")
    click.echo(f"  S3 bucket: {bucket}")
    return bucket


# ---------------------------------------------------------------------------
# Helper: IAM role (create or update)
# ---------------------------------------------------------------------------

def ensure_role(iam, role_name, trust_policy, description):
    """Create an IAM role, or update its trust policy if it already exists."""
    try:
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=description,
        )
        arn = resp['Role']['Arn']
        click.echo(f"  Created role: {arn}")
        return arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            arn = iam.get_role(RoleName=role_name)['Role']['Arn']
            iam.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(trust_policy),
            )
            click.echo(f"  Updated role: {arn}")
            return arn
        raise


# ---------------------------------------------------------------------------
# Helper: IAM inline policy (always upserts)
# ---------------------------------------------------------------------------

def put_role_policy(iam, role_name, policy_name, policy_doc):
    """Attach an inline policy to a role. Overwrites if it already exists."""
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_doc),
    )
    click.echo(f"  Attached policy '{policy_name}' to role '{role_name}'")


# ---------------------------------------------------------------------------
# Helper: Secrets Manager secret (create or update)
# ---------------------------------------------------------------------------

def ensure_secret(sm, secret_name, secret_key):
    """Store the lakeFS secret access key in Secrets Manager.

    Glue's OAuth2 integration reads the client secret from Secrets Manager
    using the reserved key name USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET.
    """
    secret_value = json.dumps({
        'USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET': secret_key,
    })
    try:
        resp = sm.create_secret(Name=secret_name, SecretString=secret_value)
        arn = resp['ARN']
        click.echo(f"  Created secret: {arn}")
        return arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceExistsException':
            sm.put_secret_value(SecretId=secret_name, SecretString=secret_value)
            arn = sm.describe_secret(SecretId=secret_name)['ARN']
            click.echo(f"  Updated secret: {arn}")
            return arn
        raise


# ---------------------------------------------------------------------------
# Helper: Glue Connection (create or update)
# ---------------------------------------------------------------------------

def ensure_connection(glue, connection_name, conn_props):
    """Create or update an ICEBERGRESTCATALOG Glue connection."""
    try:
        glue.get_connection(Name=connection_name)
        glue.update_connection(Name=connection_name, ConnectionInput={
            'Name': connection_name,
            **conn_props,
        })
        click.echo(f"  Updated connection: {connection_name}")
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityNotFoundException':
            raise
        glue.create_connection(ConnectionInput={
            'Name': connection_name,
            **conn_props,
            'ValidateCredentials': True,
        })
        click.echo(f"  Created connection: {connection_name}")


# ---------------------------------------------------------------------------
# Helper: Lake Formation resource registration
# ---------------------------------------------------------------------------

def ensure_lf_registration(lf, connection_arn, role_arn):
    """Register the Glue connection as a Lake Formation federated resource.

    This tells Lake Formation to manage credential vending for queries against
    this connection. The role must be the SAME role used by the Glue connection
    (not a separate Lake Formation role). WithPrivilegedAccess grants the
    registering principal full control.
    """
    try:
        lf.register_resource(
            ResourceArn=connection_arn,
            RoleArn=role_arn,
            WithFederation=True,
            WithPrivilegedAccess=True,
        )
        click.echo(f"  Registered: {connection_arn}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            lf.update_resource(
                RoleArn=role_arn,
                ResourceArn=connection_arn,
                WithFederation=True,
            )
            click.echo(f"  Updated registration: {connection_arn}")
        else:
            raise


# ---------------------------------------------------------------------------
# Helper: Ensure caller is a Lake Formation Data Lake Admin
# ---------------------------------------------------------------------------

def ensure_lf_admin(lf, sts):
    """Add the current caller as a Lake Formation Data Lake Admin if not already.

    Data Lake Admins bypass Lake Formation permission checks, which is needed
    to create federated catalogs and grant permissions on them.
    """
    caller_arn = sts.get_caller_identity()['Arn']
    current_settings = lf.get_data_lake_settings()['DataLakeSettings']
    admins = current_settings.get('DataLakeAdmins', [])
    admin_ids = [a['DataLakePrincipalIdentifier'] for a in admins]
    if caller_arn not in admin_ids:
        admins.append({'DataLakePrincipalIdentifier': caller_arn})
        # Only pass writable fields back - read-only fields cause errors
        writable_keys = {
            'DataLakeAdmins', 'CreateDatabaseDefaultPermissions',
            'CreateTableDefaultPermissions', 'TrustedResourceOwners',
            'AllowExternalDataFiltering', 'ExternalDataFilteringAllowList',
            'AuthorizedSessionTagValueList', 'AllowFullTableExternalDataAccess',
            'Parameters',
        }
        clean = {k: v for k, v in current_settings.items() if k in writable_keys}
        clean['DataLakeAdmins'] = admins
        lf.put_data_lake_settings(DataLakeSettings=clean)
        click.echo(f"  Added as Lake Formation admin: {caller_arn}")
    else:
        click.echo(f"  Already a Lake Formation admin: {caller_arn}")


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option('--lakefs-url', required=True,
              help='lakeFS server URL (e.g. https://my-org.us-east-1.lakefscloud.io)')
@click.option('--lakefs-repo', required=True,
              help='lakeFS repository name')
@click.option('--lakefs-branch', default='main', show_default=True,
              help='lakeFS ref to expose (branch, tag, or commit ID)')
@click.option('--lakefs-access-key-id', required=True,
              help='lakeFS access key ID (used as OAuth2 client_id)')
@click.option('--lakefs-secret-access-key', required=True,
              help='lakeFS secret access key (stored in Secrets Manager)')
@click.option('--catalog-name', default='lakefs-catalog', show_default=True,
              help='Name for the federated catalog in Glue')
@click.option('--region', default='us-east-1', show_default=True,
              help='AWS region')
@click.option('--grant-to', multiple=True,
              help='IAM role/user ARNs to grant full catalog access (repeatable)')
def setup(lakefs_url, lakefs_repo, lakefs_ref, lakefs_access_key_id,
          lakefs_secret_access_key, catalog_name, region, grant_to):
    """Set up AWS Glue Catalog Federation for a lakeFS Iceberg REST Catalog.

    Idempotent -- safe to rerun. Existing resources are updated in place.

    Creates a federated catalog that allows Athena, Redshift, and EMR to query
    Iceberg tables managed by lakeFS. Each catalog is scoped to a single
    lakeFS repository + branch combination.
    """

    lakefs_url = lakefs_url.rstrip('/')
    connection_name = f'{catalog_name}-connection'
    secret_name = f'{catalog_name}-secret'
    glue_role_name = f'{catalog_name}-GlueConnectionRole'

    # lakeFS Iceberg REST Catalog endpoints:
    # - instance_url: scoped to a specific repo+ref so Glue sees a flat
    #   namespace hierarchy (without repo.ref prefix)
    # - token_url: OAuth2 client credentials endpoint; lakeFS maps
    #   access_key_id -> client_id, secret_access_key -> client_secret
    instance_url = f'{lakefs_url}/iceberg/relative_to/{lakefs_repo}.{lakefs_ref}/api'
    token_url = f'{lakefs_url}/iceberg/api/v1/oauth/tokens'

    session = boto3.Session(region_name=region)
    sts = session.client('sts')
    iam = session.client('iam')
    sm = session.client('secretsmanager')
    glue = session.client('glue')
    lf = session.client('lakeformation')

    account_id = get_account_id(sts)
    connection_arn = f'arn:aws:glue:{region}:{account_id}:connection/{connection_name}'

    click.echo(f"Account: {account_id} | Region: {region}")
    click.echo()

    # -----------------------------------------------------------------------
    # Step 1: Discover the S3 bucket backing the lakeFS repository
    # -----------------------------------------------------------------------
    # lakeFS stores Iceberg metadata and data files in a "storage namespace"
    # which is a real S3 bucket + prefix. We need the bucket name to grant
    # the IAM role read access so Lake Formation can vend S3 credentials
    # to query engines.
    click.echo("[1/7] Resolving S3 bucket from lakeFS repository...")
    s3_bucket = get_storage_bucket(lakefs_url, lakefs_access_key_id, lakefs_secret_access_key, lakefs_repo)
    click.echo()

    # -----------------------------------------------------------------------
    # Step 2: Store lakeFS credentials in Secrets Manager
    # -----------------------------------------------------------------------
    # Glue's OAuth2 integration retrieves the client secret from Secrets
    # Manager at runtime. The secret must use the reserved key name
    # USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET.
    click.echo("[2/7] Ensuring Secrets Manager secret...")
    secret_arn = ensure_secret(sm, secret_name, lakefs_secret_access_key)
    click.echo()

    # -----------------------------------------------------------------------
    # Step 3: Create the IAM role
    # -----------------------------------------------------------------------
    # A single role is used for both the Glue connection and Lake Formation
    # registration. This matches the AWS Console's behavior - using separate
    # roles causes federation to break.
    #
    # The role needs:
    #   - Trust from glue.amazonaws.com (to make REST API calls to lakeFS)
    #   - Trust from lakeformation.amazonaws.com (to vend S3 credentials)
    #   - Secrets Manager read access (to retrieve lakeFS OAuth2 secret)
    #   - S3 read access on the storage bucket (to read Iceberg data files)
    click.echo("[3/7] Ensuring IAM role...")

    glue_role_arn = ensure_role(iam, glue_role_name, {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Principal': {'Service': 'glue.amazonaws.com'},
                'Action': 'sts:AssumeRole',
                'Condition': {'StringEquals': {'aws:SourceAccount': account_id}},
            },
            {
                'Effect': 'Allow',
                'Principal': {'Service': 'lakeformation.amazonaws.com'},
                'Action': 'sts:AssumeRole',
            },
        ],
    }, 'Allows AWS Glue and Lake Formation to access lakeFS catalog federation resources')

    put_role_policy(iam, glue_role_name, 'SecretsManagerAccess', {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Action': ['secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret', 'secretsmanager:PutSecretValue'],
            'Resource': [f'arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_name}*'],
        }],
    })

    put_role_policy(iam, glue_role_name, 'S3DataAccess', {
        'Version': '2012-10-17',
        'Statement': [
            {'Effect': 'Allow', 'Action': ['s3:GetObject'], 'Resource': [f'arn:aws:s3:::{s3_bucket}/*']},
            {'Effect': 'Allow', 'Action': ['s3:ListBucket'], 'Resource': [f'arn:aws:s3:::{s3_bucket}']},
        ],
    })

    click.echo("  Waiting for IAM propagation...")
    time.sleep(10)
    click.echo()

    # -----------------------------------------------------------------------
    # Step 4: Create the Glue connection to lakeFS
    # -----------------------------------------------------------------------
    # Connection type ICEBERGRESTCATALOG tells Glue this is a generic Iceberg
    # REST catalog (as opposed to SNOWFLAKEICEBERGRESTCATALOG or
    # DATABRICKSICEBERGRESTCATALOG which enforce vendor-specific URL patterns).
    #
    # The connection uses OAuth2 client credentials flow:
    #   - client_id = lakeFS access key ID
    #   - client_secret = lakeFS secret key (from Secrets Manager)
    #   - token endpoint = /iceberg/api/v1/oauth/tokens
    #
    # ValidateCredentials=True tells Glue to verify OAuth2 credentials
    # during creation, failing fast if the lakeFS endpoint is unreachable.
    click.echo("[4/7] Ensuring Glue connection...")
    conn_props = {
        'ConnectionType': 'ICEBERGRESTCATALOG',
        'ConnectionProperties': {
            'INSTANCE_URL': instance_url,
            'ROLE_ARN': glue_role_arn,
            'CATALOG_CASING_FILTER': 'LOWERCASE_ONLY',
        },
        'AuthenticationConfiguration': {
            'AuthenticationType': 'OAUTH2',
            'OAuth2Properties': {
                'OAuth2GrantType': 'CLIENT_CREDENTIALS',
                'TokenUrl': token_url,
                'OAuth2ClientApplication': {
                    'UserManagedClientApplicationClientId': lakefs_access_key_id,
                },
                'TokenUrlParametersMap': {
                    'scope': 'CLIENT_CREDENTIALS',
                },
            },
            'SecretArn': secret_arn,
        },
    }
    ensure_connection(glue, connection_name, conn_props)
    click.echo()

    # -----------------------------------------------------------------------
    # Step 5: Register the connection with Lake Formation
    # -----------------------------------------------------------------------
    # This tells Lake Formation to manage S3 credential vending for queries
    # against this federated catalog. When Athena queries a federated table,
    # Lake Formation reads the table's S3 location from lakeFS metadata,
    # then vends temporary, scoped S3 credentials to Athena.
    #
    # The caller must be a Data Lake Admin to register resources and create
    # federated catalogs.
    click.echo("[5/7] Ensuring Lake Formation registration...")
    ensure_lf_admin(lf, sts)
    ensure_lf_registration(lf, connection_arn, glue_role_arn)
    click.echo()

    # -----------------------------------------------------------------------
    # Step 6: Create the federated catalog
    # -----------------------------------------------------------------------
    # The federated catalog appears in Athena's catalog selector and in the
    # Lake Formation console. It references the Glue connection and the
    # remote catalog identifier (the lakeFS repository name).
    #
    # DataLakeAccessProperties is included to match the AWS Console's
    # behavior, though federation works without it.
    click.echo("[6/7] Ensuring federated catalog...")
    try:
        glue.create_catalog(
            Name=catalog_name,
            CatalogInput={
                'CatalogProperties': {
                    'DataLakeAccessProperties': {},
                },
                'CreateDatabaseDefaultPermissions': [],
                'CreateTableDefaultPermissions': [],
                'FederatedCatalog': {
                    'ConnectionName': connection_name,
                    'Identifier': lakefs_repo,
                },
            },
        )
        click.echo(f"  Created catalog: {catalog_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            click.echo(f"  Catalog already exists: {catalog_name}")
        else:
            raise
    click.echo()

    # -----------------------------------------------------------------------
    # Step 7: Grant Lake Formation permissions on the catalog
    # -----------------------------------------------------------------------
    # For the catalog to be visible in the Lake Formation UI and queryable
    # by users, principals need explicit Lake Formation grants. SUPER_USER
    # is required for the catalog to appear in the console.
    #
    # Permissions are granted to:
    #   - The caller (whoever runs this script)
    #   - Any additional principals specified via --grant-to
    catalog_id = f'{account_id}:{catalog_name}'
    caller_arn = sts.get_caller_identity()['Arn']
    principals = set(grant_to) | {caller_arn}

    click.echo("[7/7] Granting Lake Formation catalog permissions...")
    all_perms = ['ALL', 'ALTER', 'CREATE_CATALOG', 'CREATE_DATABASE', 'DESCRIBE', 'DROP', 'SUPER_USER']
    grantable = ['ALL', 'ALTER', 'CREATE_CATALOG', 'CREATE_DATABASE', 'DESCRIBE', 'DROP']
    for principal in principals:
        try:
            lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal},
                Resource={'Catalog': {'Id': catalog_id}},
                Permissions=all_perms,
                PermissionsWithGrantOption=grantable,
            )
            click.echo(f"  Granted to: {principal}")
        except ClientError as e:
            click.echo(f"  {principal}: {e.response['Error']['Message']}")
    click.echo()

    # -----------------------------------------------------------------------
    # Done
    # -----------------------------------------------------------------------
    click.secho("Setup complete!", fg='green', bold=True)
    click.echo()
    click.echo(f"  Catalog:    {catalog_name}")
    click.echo(f"  Connection: {connection_name}")
    click.echo(f"  S3 bucket:  {s3_bucket}")
    click.echo(f"  Endpoint:   {instance_url}")
    click.echo()
    click.echo("Query from Athena:")
    click.echo(f'  SELECT * FROM "{catalog_name}"."<namespace>"."<table>" LIMIT 10;')


if __name__ == '__main__':
    setup()
