#!/usr/bin/env python3
"""List and delete federated catalogs, their connections, and Lake Formation registrations."""

import boto3
import click
from botocore.exceptions import ClientError

@click.command()
@click.option('--region', default='us-east-1', show_default=True)
@click.option('--yes', is_flag=True, help='Skip confirmation prompt')
def cleanup(region, yes):
    """Remove federated catalogs and their associated resources."""

    glue = boto3.client('glue', region_name=region)
    lf = boto3.client('lakeformation', region_name=region)
    sts = boto3.client('sts', region_name=region)
    account_id = sts.get_caller_identity()['Account']

    # List all catalogs
    catalogs = glue.get_catalogs()['CatalogList']
    federated = [c for c in catalogs if 'FederatedCatalog' in c and c.get('FederatedCatalog', {}).get('ConnectionType') == 'ICEBERGRESTCATALOG']

    if not federated:
        click.echo("No ICEBERGRESTCATALOG federated catalogs found.")
        return

    click.echo(f"Found {len(federated)} federated catalog(s):\n")
    for i, cat in enumerate(federated):
        name = cat['Name']
        conn = cat['FederatedCatalog'].get('ConnectionName', '?')
        ident = cat['FederatedCatalog'].get('Identifier', '?')
        click.echo(f"  [{i+1}] {name}  (connection: {conn}, identifier: {ident})")

    click.echo()
    if not yes:
        if not click.confirm("Delete all of these?"):
            click.echo("Aborted.")
            return

    for cat in federated:
        name = cat['Name']
        catalog_id = f'{account_id}:{name}'
        conn_name = cat['FederatedCatalog'].get('ConnectionName', '')
        conn_arn = f'arn:aws:glue:{region}:{account_id}:connection/{conn_name}'

        click.echo(f"\n  Deleting {name}...")

        # 1. Delete catalog
        try:
            glue.delete_catalog(CatalogId=catalog_id)
            click.echo(f"    Deleted catalog: {catalog_id}")
        except ClientError as e:
            click.echo(f"    Catalog: {e.response['Error']['Message']}")

        # 2. Deregister from Lake Formation
        try:
            lf.deregister_resource(ResourceArn=conn_arn)
            click.echo(f"    Deregistered LF resource: {conn_arn}")
        except ClientError as e:
            click.echo(f"    LF resource: {e.response['Error']['Message']}")

        # 3. Delete connection
        try:
            glue.delete_connection(ConnectionName=conn_name)
            click.echo(f"    Deleted connection: {conn_name}")
        except ClientError as e:
            click.echo(f"    Connection: {e.response['Error']['Message']}")

    click.echo("\nDone.")


if __name__ == '__main__':
    cleanup()
