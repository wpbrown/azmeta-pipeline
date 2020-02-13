from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.billing import BillingManagementClient
import itertools
import datetime
import time
import sys
import configparser
from collections import namedtuple


#### Monkey Patching Bugs in SDK ####
def _download_initial_monkey(self, scope, metric=None, custom_headers=None, raw=False, **operation_config):
    import uuid # Monkey: import for monkey
    from msrest.pipeline import ClientRawResponse # Monkey: import for monkey
    from azure.mgmt.consumption import models # Monkey: import for monkey

    # Construct URL
    url = self.download.metadata['url']
    path_format_arguments = {
        'scope': self._serialize.url("scope", scope, 'str', skip_quote=True)
    }
    url = self._client.format_url(url, **path_format_arguments)

    # Construct parameters
    query_parameters = {}
    query_parameters['api-version'] = self._serialize.query("self.api_version", self.api_version, 'str')
    if metric is not None:
        query_parameters['metric'] = self._serialize.query("metric", metric, 'str')

    # Construct headers
    header_parameters = {}
    header_parameters['Accept'] = 'application/json'
    if self.config.generate_client_request_id:
        header_parameters['x-ms-client-request-id'] = str(uuid.uuid1())
    if custom_headers:
        header_parameters.update(custom_headers)
    if self.config.accept_language is not None:
        header_parameters['accept-language'] = self._serialize.header("self.config.accept_language", self.config.accept_language, 'str')

    # Construct and send request
    request = self._client.get(url, query_parameters, header_parameters) # Monkey: change POST to GET
    response = self._client.send(request, stream=False, **operation_config)
    response.request.method = 'POST' # Monkey: report that we did a POST

    if response.status_code not in [200, 202]:
        raise models.ErrorResponseException(self._deserialize, response)

    deserialized = None
    header_dict = {}

    if response.status_code == 200:
        deserialized = self._deserialize('UsageDetailsDownloadResponse', response)
        header_dict = {
            'Location': 'str',
            'Retry-After': 'str',
            'Azure-AsyncOperation': 'str',
        }

    if raw:
        client_raw_response = ClientRawResponse(deserialized, response)
        client_raw_response.add_headers(header_dict)
        return client_raw_response

    return deserialized


from azure.mgmt.consumption.operations.usage_details_operations import UsageDetailsOperations
from msrestazure.polling import arm_polling

UsageDetailsOperations._download_initial = _download_initial_monkey # Monkey: apply above function
arm_polling.FINISHED = frozenset(['succeeded', 'canceled', 'failed', 'completed']) # Monkey: detect completed as valid status
arm_polling.SUCCEEDED = frozenset(['succeeded', 'completed']) # Monkey: detect completed as valid status
#### /Monkey Patching Bugs in SDK ####


def select_billing_period_name(subscription_id: str) -> str:
    b_client = get_client_from_cli_profile(BillingManagementClient, subscription_id = subscription_id)
    top_billing_periods_paged = b_client.billing_periods.list(top=5)
    top_billing_periods = itertools.islice(top_billing_periods_paged, 5)

    today = datetime.date.today()
    active_period = next(top_billing_periods)
    while today <= active_period.billing_period_end_date:
        active_period = next(top_billing_periods)

    print('Selected billing period: {} ({} - {})'.format(active_period.name, active_period.billing_period_start_date, active_period.billing_period_end_date))      
    return active_period.name                             


def generate_usage_blob_data(billing_account_name: str, billing_period: str) -> str:
    cm_client = get_client_from_cli_profile(ConsumptionManagementClient)
    download_operation = cm_client.usage_details.download('/providers/Microsoft.Billing/billingAccounts/{}/providers/Microsoft.Billing/billingPeriods/{}'.format(billing_account_name, billing_period), metric='amortizedcost')
    while not download_operation.done():
        download_operation.wait(30)
        print('Generate data status: {}'.format(download_operation.status()))
    download_result = download_operation.result()
    print('Got URL to blob: {}'.format(download_result.download_url))
    return download_result.download_url


ResourceId = namedtuple('ResourceId', 'subscription, resource_group, name')


def start_pipeline(data_factory: ResourceId, blob_name: str, blob_url: str) -> str:
    df_client = get_client_from_cli_profile(DataFactoryManagementClient, subscription_id = data_factory.subscription)
    pipeline_paramters = {
        'FullBlobUrl': blob_url,
        'BlobName': blob_name
    }
    create_result = df_client.pipelines.create_run(data_factory.resource_group, data_factory.name, 'pipeline_ingestusage', parameters=pipeline_paramters)
    print('Created Ingestion Pipeline Run: {}'.format(create_result.run_id))
    return create_result.run_id


def watch_pipeline(data_factory: ResourceId, id: str):
    df_client = get_client_from_cli_profile(DataFactoryManagementClient, subscription_id = data_factory.subscription)
    while True:
        time.sleep(30)
        run = df_client.pipeline_runs.get(data_factory.resource_group, data_factory.name, id)        
        print('Pipeline status: {} {}'.format(run.status, run.message))
        if run.status not in ('InProgress', 'Queued'):
            break

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    config = config['default']

    subscription_id = config['usageSubscription']
    billing_account_name = config['billingAccountName']
    data_factory = ResourceId(subscription_id, config['usageResourceGroup'], config['usageDataFactoryName'])

    if len(sys.argv) > 2 and sys.argv[2]:
        active_period_name = sys.argv[2]
        print('Using provided billing period: {}'.format(active_period_name))
    else:
        print('Selecting billing period...')
        active_period_name = select_billing_period_name(subscription_id)

    print('Generating usage data (this can take 5 to 10 minutes)...')
    generated_blob_url = generate_usage_blob_data(billing_account_name, active_period_name)

    print('Starting Data Factory Ingestion Pipeline...')
    run_id = start_pipeline(data_factory, active_period_name, generated_blob_url)

    print('Monitoring Pipeline...')
    watch_pipeline(data_factory, run_id)
