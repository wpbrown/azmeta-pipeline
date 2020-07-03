# azmeta-pipeline

This ARM template creates an environment for automatically ingesting Azure usage data in to an Azure Data Explorer cluster. This is useful in large accounts with millions of usage records that need to be explored and/or processed. It is also useful for any size account where custom processing is required for [cost allocation and allotment](https://github.com/wpbrown/azmeta-docs) beyond what is possible in the Cost Management portal with tags, scopes, and other discriminators.

The pipeline compresses and stores the [Usage Detail v2](https://docs.microsoft.com/en-us/azure/cost-management-billing/manage/consumption-api-overview#usage-details-api) CSV data in to a storage account for archival purposes and loads it to Azure Data Explorer for online analysis.

## Using the Data

This database is meant to be used with the azmeta ecosystem of tools such as an [azmeta-codespace](https://github.com/wpbrown/azmeta-codespace).

You can also connect to your new azmeta database using Azure Data Explorer's own [native tools](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/tools/) such as the [Azure Data Explorer Web UI](https://dataexplorer.azure.com/), [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio/notebooks-kqlmagic?view=sql-server-ver15#kqlmagic-with-azure-monitor-logs), [Kusto Explorer](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/tools/kusto-explorer), or the [Kusto CLI](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/tools/kusto-cli). 

## Limitations

Currently the Enterprise Agreement [billing account type](https://docs.microsoft.com/en-us/azure/cost-management-billing/cost-management-billing-overview#billing-accounts) are supported. The new Microsoft Customer Agreement type is not yet supported.

# Architecture

The system consists of an Azure Data Explorer cluster, an Azure Data Factory instance, and a storage account. An admin using an Azure CLI extension or Azure Cost Management automatic export push usage data in to blob containers in the storage account. Azure Data Factory will automatically push new data in to Azure Data Explorer as it arrives in the blob containers.

![img](docs/images/usage-pipeline.svg)

## Data Flows

Usage data for a closed billing period will be automatically imported in to Azure Data Explorer once it is exported by the Azure Cost Management on the 5th day of the month. This data will be imported in to the `Usage` table and is considered an immutable record. This Azure Data Factory pipeline will compress and archive the usage data in the storage account before it is loaded to Azure Data Explorer.

Month-to-date usage data for the currently open billing period will be automatically import on a nightly schedule as it exported. This data will be imported in to the `UsagePreliminary` table and all records are subject to change until the billing period has closed. 

# Installation

This process requires an Azure subscription, resource group, and service principal in the same Azure tenant as the users with access to the billing account. 

## Prerequisites

* A subscription with the resource providers `Microsoft.Storage`, `Microsoft.ContainerInstance`, and `Microsoft.EventGrid` already registered. [Register the providers](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/resource-providers-and-types#azure-cli).
* A resource group is required to deploy in to. [Create a resource group](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-cli#create-resource-groups).
* A user-assigned managed identity is required during deployment [Create a user-assigned managed identity](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-manage-ua-identity-cli). This can be created in the resource group mentioned above, however it is not required to be located there. *This resource can be deleted after deployment of the template is complete.*
* A service principal with a password/key is required for Azure Data Factory to connect to Azure Data Explorer. [Create a service principal](https://docs.microsoft.com/en-us/cli/azure/create-an-azure-service-principal-azure-cli?view=azure-cli-latest#password-based-authentication). Azure Data Factory does not currently support connecting Azure Data Explorer via managed identity.

## Deploy the ARM Template

[Deploy](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fwpbrown%2Fazmeta-pipeline%2Fmaster%2Fazuredeploy.json) `azuredeploy.json` from the root of this git repository using the [Azure Portal](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fwpbrown%2Fazmeta-pipeline%2Fmaster%2Fazuredeploy.json), [Azure CLI](https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/deploy-cli), or [PowerShell](https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/deploy-powershell).

### Parameter Reference

Name | Description
--- | ---
**kustoIngestClientId** | *Required*: The client/app id (not object id) for the service principal that the data factory will use to connect to Kusto.
**kustoIngestClientSecret** | *Required*: The client secret for the service principal that the data factory will use to connect to Kusto.
**deploymentIdentity** | *Required*: The resource id of the user-assigned managed identity resource to use during deployment. Example `/subscriptions/{GUID}/resourcegroups/{GROUP_NAME}/` `providers/Microsoft.ManagedIdentity/userAssignedIdentities/{NAME}`
kustoClusterName | Globally unique name for the Kusto cluster. Lowercase letters and numbers. Start with letter. 4-22 characters.
kustoDatabaseName | Name for the azmeta database in your Kusto cluster.
storageAccountName | Globally unique name for the storage account. Lowercase letters and numbers. 3-24 characters.
dataFactoryName | Globally unique name for the data factory. Alphanumerics and hyphens. Start and end with alphanumeric. 3-63 characters.
preProduction | Deploy for pre-production use. Uses development (no-SLA) SKU for Azure Data Explorer.
_artifactsLocation | The base URI where artifacts required by this template are located including a trailing '/'. This defaults to the latest release artifact location in GitHub. You may choose to mirror these artifacts for security/audit reasons. Use this parameter to provide your mirror.

### Azure CLI Tutorial

This tutorial assumes you are using Bash. If you are not using the Azure Cloud Shell, sign in to Azure CLI with `az login`.

Ensure the correct subscription is the default (`az account show`). You can change the default with `az account set`.

```bash
# Basic Info
RG_NAME="azmetapipeline-test-rg"
SUB_ID=$(az account show --query id -o tsv)
LOCATION="eastus2"

# Ensure the subscription is ready
az provider register -n 'Microsoft.Storage' --wait
az provider register -n 'Microsoft.ContainerInstance' --wait
az provider register -n 'Microsoft.EventGrid' --wait

# Create the RG
az group create -n $RG_NAME -l $LOCATION

# Create the service principal for Kusto access
read -d "\n" -r SP_AID SP_SECRET \
  <<<$(az ad sp create-for-rbac -n "http://azmetapipeline-test-sp" --skip-assignment --query "[appId,password]" -o tsv)

# Create the user assigned managed identity and grant it access to the RG
read -d "\n" -r MUID_RID MUID_PID \
  <<<$(az identity create -g $RG_NAME -n "deploy-muid" --query "[id,principalId]" -o tsv)
az role assignment create --assignee $MUID_PID --role "Contributor" \
  --scope "/subscriptions/$SUB_ID/resourceGroups/$RG_NAME"

# Deploy the template
az deployment group create -g $RG_NAME \
  --template-uri "https://raw.githubusercontent.com/wpbrown/azmeta-pipeline/master/azuredeploy.json" \
  --parameters \
  "deploymentIdentity=$MUID_RID" \
  "kustoIngestClientId=$SP_AID" \
  "kustoIngestClientSecret=@"<(echo $SP_SECRET)
```

Once the template deployment is complete, you can configure automatic data loading in the next section or [manually load data](#manual-data-loading) with azmpcli.

## Configure Exports in Cost Management

Two export rules need to be created in the [export blade](https://portal.azure.com/#blade/Microsoft_Azure_CostManagement/Menu/exports), one for the closed (or 'final') data and one for the open (or 'preliminary') data.

*Exact process is TBD.*

Within 5 to 10 minutes of new data being exported in to your storage account, you should see the data appearing in your Azure Data Explorer tables.

# Uninstallation

* Delete the resource group.
* Delete the deployment user-assigned managed identity (if it was created outside of the resource group).
* Delete the deployment artifacts (if they were downloaded from GitHub and stored outside of the resource group).
* Delete the service principal created for access to Azure Data Explorer.
* Delete both export configurations in the Cost Management [export blade](https://portal.azure.com/#blade/Microsoft_Azure_CostManagement/Menu/exports).

# Manual Data Loading

You may want to manually load data, for example to back-fill data from earlier billing periods. This can be accomplished with the [azmpcli](https://github.com/wpbrown/azmeta-pipeline-cli) tool. 

EA billing accounts use the EA Portal to [assign roles](https://docs.microsoft.com/en-us/azure/cost-management-billing/manage/ea-portal-get-started#enterprise-user-roles). Because these roles can not currently be assigned to service principal (application identity), an EA admin user must use this tool.

This process requires an account that has:

 * Reader or higher access to any subscription in the EA.
 * Storage Blob Data Contributor or higher access to the storage account.
   * Contributor is not sufficient. You must have Storage Blob Data Contributor.
 * Enterprise Admin rights (read-only is sufficient) in the EA portal

Install the tool in the Azure Cloud Shell as described [here](https://github.com/wpbrown/azmeta-pipeline-cli#installation-in-azure-cloud-shell). 

To ingest the first 3 billing periods of 2020:

```shell
demo@Azure:~$ ./azmpcli -s <STORAGE_ACCOUNT_NAME> 202001 202002 202003
```

You must supply the storage account name created by your ARM template deployment. If you do not specify any billing period names, the tool will automatically select the latest closed billing period.

If you have access to multiple EA billing accounts you must specify the EA account number. 

```shell
demo@Azure:~$ ./azmpcli -a <EA_ACCOUNT_NUMBER> -s <STORAGE_ACCOUNT_NAME>
```
