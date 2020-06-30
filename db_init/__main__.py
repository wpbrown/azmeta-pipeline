from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoAuthenticationError
import sys
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, after_log, retry_if_exception

logging.basicConfig(level=logging.DEBUG)

logging.info('Init starting.')

init_script = """
.create table {TABLE_BASE}Ingest (BillingAccountId: int, BillingAccountName: string, BillingPeriodStartDate: datetime, BillingPeriodEndDate: datetime, BillingProfileId: int, BillingProfileName: string, AccountOwnerId: string, AccountName: string, SubscriptionId: guid, SubscriptionName: string, Date: datetime, Product: string, PartNumber: string, MeterId: guid, ServiceFamily: string, MeterCategory: string, MeterSubCategory: string, MeterRegion: string, MeterName: string, Quantity: decimal, EffectivePrice: decimal, Cost: decimal, UnitPrice: decimal, BillingCurrency: string, ResourceLocation: string, AvailabilityZone: string, ConsumedService: string, ResourceId: string, ResourceName: string, ServiceInfo1: string, ServiceInfo2: string, AdditionalInfo: dynamic, Tags: string,  InvoiceSectionId: string, InvoiceSection: string, CostCenter: int, UnitOfMeasure: string, ResourceGroup: string, ReservationId: guid, ReservationName: string, ProductOrderId: guid, ProductOrderName: string, OfferId: string, IsAzureCreditEligible: string, Term: string, PublisherName: string, PlanName: string, ChargeType: string, Frequency: string, PublisherType: string) 

.create table {TABLE_BASE}Ingest ingestion csv mapping '{TABLE_BASE}Mapping' '[{"Name":"BillingAccountId","DataType":"int","Ordinal":"0","ConstValue":null},{"Name":"BillingAccountName","DataType":"string","Ordinal":"1","ConstValue":null},{"Name":"BillingPeriodStartDate","DataType":"datetime","Ordinal":"2","ConstValue":null},{"Name":"BillingPeriodEndDate","DataType":"datetime","Ordinal":"3","ConstValue":null},{"Name":"BillingProfileId","DataType":"int","Ordinal":"4","ConstValue":null},{"Name":"BillingProfileName","DataType":"string","Ordinal":"5","ConstValue":null},{"Name":"AccountOwnerId","DataType":"string","Ordinal":"6","ConstValue":null},{"Name":"AccountName","DataType":"string","Ordinal":"7","ConstValue":null},{"Name":"SubscriptionId","DataType":"guid","Ordinal":"8","ConstValue":null},{"Name":"SubscriptionName","DataType":"string","Ordinal":"9","ConstValue":null},{"Name":"Date","DataType":"datetime","Ordinal":"10","ConstValue":null},{"Name":"Product","DataType":"string","Ordinal":"11","ConstValue":null},{"Name":"PartNumber","DataType":"string","Ordinal":"12","ConstValue":null},{"Name":"MeterId","DataType":"guid","Ordinal":"13","ConstValue":null},{"Name":"ServiceFamily","DataType":"string","Ordinal":"14","ConstValue":null},{"Name":"MeterCategory","DataType":"string","Ordinal":"15","ConstValue":null},{"Name":"MeterSubCategory","DataType":"string","Ordinal":"16","ConstValue":null},{"Name":"MeterRegion","DataType":"string","Ordinal":"17","ConstValue":null},{"Name":"MeterName","DataType":"string","Ordinal":"18","ConstValue":null},{"Name":"Quantity","DataType":"decimal","Ordinal":"19","ConstValue":null},{"Name":"EffectivePrice","DataType":"decimal","Ordinal":"20","ConstValue":null},{"Name":"Cost","DataType":"decimal","Ordinal":"21","ConstValue":null},{"Name":"UnitPrice","DataType":"decimal","Ordinal":"22","ConstValue":null},{"Name":"BillingCurrency","DataType":"string","Ordinal":"23","ConstValue":null},{"Name":"ResourceLocation","DataType":"string","Ordinal":"24","ConstValue":null},{"Name":"AvailabilityZone","DataType":"string","Ordinal":"25","ConstValue":null},{"Name":"ConsumedService","DataType":"string","Ordinal":"26","ConstValue":null},{"Name":"ResourceId","DataType":"string","Ordinal":"27","ConstValue":null},{"Name":"ResourceName","DataType":"string","Ordinal":"28","ConstValue":null},{"Name":"ServiceInfo1","DataType":"string","Ordinal":"29","ConstValue":null},{"Name":"ServiceInfo2","DataType":"string","Ordinal":"30","ConstValue":null},{"Name":"AdditionalInfo","DataType":"dynamic","Ordinal":"31","ConstValue":null},{"Name":"Tags","DataType":"string","Ordinal":"32","ConstValue":null},{"Name":"InvoiceSectionId","DataType":"string","Ordinal":"33","ConstValue":null},{"Name":"InvoiceSection","DataType":"string","Ordinal":"34","ConstValue":null},{"Name":"CostCenter","DataType":"int","Ordinal":"35","ConstValue":null},{"Name":"UnitOfMeasure","DataType":"string","Ordinal":"36","ConstValue":null},{"Name":"ResourceGroup","DataType":"string","Ordinal":"37","ConstValue":null},{"Name":"ReservationId","DataType":"guid","Ordinal":"38","ConstValue":null},{"Name":"ReservationName","DataType":"string","Ordinal":"39","ConstValue":null},{"Name":"ProductOrderId","DataType":"guid","Ordinal":"40","ConstValue":null},{"Name":"ProductOrderName","DataType":"string","Ordinal":"41","ConstValue":null},{"Name":"OfferId","DataType":"string","Ordinal":"42","ConstValue":null},{"Name":"IsAzureCreditEligible","DataType":"string","Ordinal":"43","ConstValue":null},{"Name":"Term","DataType":"string","Ordinal":"44","ConstValue":null},{"Name":"PublisherName","DataType":"string","Ordinal":"45","ConstValue":null},{"Name":"PlanName","DataType":"string","Ordinal":"46","ConstValue":null},{"Name":"ChargeType","DataType":"string","Ordinal":"47","ConstValue":null},{"Name":"Frequency","DataType":"string","Ordinal":"48","ConstValue":null},{"Name":"PublisherType","DataType":"string","Ordinal":"49","ConstValue":null}]'

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

logging.info('Cluster:', cluster_url)
logging.info('Database:', db_name)

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