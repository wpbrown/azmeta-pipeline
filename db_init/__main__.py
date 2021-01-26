from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoAuthenticationError
import sys
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, after_log, retry_if_exception

logging.basicConfig(level=logging.DEBUG)

logging.info('Init starting.')

init_script = """
.create table {TABLE_BASE}Ingest (['InvoiceSectionName']:string, ['AccountName']:string, ['AccountOwnerId']:string, ['SubscriptionId']:string, ['SubscriptionName']:string, ['ResourceGroup']:string, ['ResourceLocation']:string, ['Date']:datetime, ['ProductName']:string, ['MeterCategory']:string, ['MeterSubCategory']:string, ['MeterId']:string, ['MeterName']:string, ['MeterRegion']:string, ['UnitOfMeasure']:string, ['Quantity']:decimal, ['EffectivePrice']:decimal, ['CostInBillingCurrency']:decimal, ['CostCenter']:string, ['ConsumedService']:string, ['ResourceId']:string, ['Tags']:string, ['OfferId']:string, ['AdditionalInfo']:dynamic, ['ServiceInfo1']:string, ['ServiceInfo2']:string, ['ResourceName']:string, ['ReservationId']:string, ['ReservationName']:string, ['UnitPrice']:decimal, ['ProductOrderId']:string, ['ProductOrderName']:string, ['Term']:string, ['PublisherType']:string, ['PublisherName']:string, ['ChargeType']:string, ['Frequency']:string, ['PricingModel']:string, ['AvailabilityZone']:string, ['BillingAccountId']:string, ['BillingAccountName']:string, ['BillingCurrencyCode']:string, ['BillingPeriodStartDate']:datetime, ['BillingPeriodEndDate']:datetime, ['BillingProfileId']:string, ['BillingProfileName']:string, ['InvoiceSectionId']:string, ['IsAzureCreditEligible']:string, ['PartNumber']:string, ['PayGPrice']:decimal, ['PlanName']:string, ['ServiceFamily']:string)

.create table {TABLE_BASE}Ingest ingestion csv mapping '{TABLE_BASE}Mapping' '[{"Name":"InvoiceSectionName","Ordinal":0},{"Name":"AccountName","Ordinal":1},{"Name":"AccountOwnerId","Ordinal":2},{"Name":"SubscriptionId","Ordinal":3},{"Name":"SubscriptionName","Ordinal":4},{"Name":"ResourceGroup","Ordinal":5},{"Name":"ResourceLocation","Ordinal":6},{"Name":"Date","Ordinal":7},{"Name":"ProductName","Ordinal":8},{"Name":"MeterCategory","Ordinal":9},{"Name":"MeterSubCategory","Ordinal":10},{"Name":"MeterId","Ordinal":11},{"Name":"MeterName","Ordinal":12},{"Name":"MeterRegion","Ordinal":13},{"Name":"UnitOfMeasure","Ordinal":14},{"Name":"Quantity","Ordinal":15},{"Name":"EffectivePrice","Ordinal":16},{"Name":"CostInBillingCurrency","Ordinal":17},{"Name":"CostCenter","Ordinal":18},{"Name":"ConsumedService","Ordinal":19},{"Name":"ResourceId","Ordinal":20},{"Name":"Tags","Ordinal":21},{"Name":"OfferId","Ordinal":22},{"Name":"AdditionalInfo","Ordinal":23},{"Name":"ServiceInfo1","Ordinal":24},{"Name":"ServiceInfo2","Ordinal":25},{"Name":"ResourceName","Ordinal":26},{"Name":"ReservationId","Ordinal":27},{"Name":"ReservationName","Ordinal":28},{"Name":"UnitPrice","Ordinal":29},{"Name":"ProductOrderId","Ordinal":30},{"Name":"ProductOrderName","Ordinal":31},{"Name":"Term","Ordinal":32},{"Name":"PublisherType","Ordinal":33},{"Name":"PublisherName","Ordinal":34},{"Name":"ChargeType","Ordinal":35},{"Name":"Frequency","Ordinal":36},{"Name":"PricingModel","Ordinal":37},{"Name":"AvailabilityZone","Ordinal":38},{"Name":"BillingAccountId","Ordinal":39},{"Name":"BillingAccountName","Ordinal":40},{"Name":"BillingCurrencyCode","Ordinal":41},{"Name":"BillingPeriodStartDate","Ordinal":42},{"Name":"BillingPeriodEndDate","Ordinal":43},{"Name":"BillingProfileId","Ordinal":44},{"Name":"BillingProfileName","Ordinal":45},{"Name":"InvoiceSectionId","Ordinal":46},{"Name":"IsAzureCreditEligible","Ordinal":47},{"Name":"PartNumber","Ordinal":48},{"Name":"PayGPrice","Ordinal":49},{"Name":"PlanName","Ordinal":50},{"Name":"ServiceFamily","Ordinal":51}]'

.create function 
IngestTo{TABLE_BASE}()
{
    {TABLE_BASE}Ingest
    | extend Tags = todynamic(strcat('{', Tags, '}')), ResourceId = tolower(ResourceId)
}

.set-or-append {TABLE_BASE} <| IngestTo{TABLE_BASE}() | limit 0

.alter-merge table {TABLE_BASE}Ingest policy retention softdelete = timespan(0) recoverability = disabled

.alter table {TABLE_BASE} policy update
@'[{"IsEnabled": true, "Source": "{TABLE_BASE}Ingest", "Query": "IngestTo{TABLE_BASE}()", "IsTransactional": true, "PropagateIngestionProperties": true}]'
"""

cluster_url = sys.argv[1]
db_name = sys.argv[2]

logging.info('Cluster: %s', cluster_url)
logging.info('Database: %s', db_name)

logging.info('Creating connection string.')
kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url)

logging.info('Creating client.')
client = KustoClient(kcsb)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=32), retry=retry_if_exception(lambda x:not isinstance(x, KustoServiceError)), after=after_log(logging, logging.DEBUG))
def execute_command(command: str) -> None:
    client.execute_mgmt(db_name, command)
    

def create_table_set(base_name: str) -> None:
    commands = init_script.replace('{TABLE_BASE}', base_name).split('\n\n')
    for command in commands:
        execute_command(command)
        

logging.info('Creating Usage Tables.')
create_table_set('Usage')

logging.info('Creating UsagePreliminary Tables.')
create_table_set('UsagePreliminary')

logging.info('Init complete.')