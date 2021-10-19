from clients.dynamodb import DyanmoDBClient
from config import HTS_CODES
from clients.exclusion_requests import ExclusionRequestsClient
from tqdm import tqdm
import os

def write_batch(batch, dynamo, to_table, id_field, **kwargs):
    if kwargs['verbose_logging']:
        print(f'Writing batch of {len(batch)} items to database')
    dynamo.client.batch_write_item(
        RequestItems={
            to_table: [
                {
                    'PutRequest': {
                        'Item': {
                            'ID': document['M'][id_field],
                            'Details': document
                        }
                    }
                } for document in batch
            ]
        }
    )

def __main__():
    # LOGGING
    verbose_logging = bool(os.environ.get('VERBOSE_LOGGING', False))
    idx = 0

    # SETUP
    erc = ExclusionRequestsClient()
    dynamo = DyanmoDBClient()

    # PROCESS EXCLUSION REQUESTS
    dynamo.with_table('exclusion_requests', [('S', 'ID')], 'ID')
    print('Extracting and loading exclusion requests...')
    hts_code_iterator = HTS_CODES if verbose_logging else tqdm(HTS_CODES)
    for hts_code in hts_code_iterator:
        summaries = erc.get_summaries(hts_code)
        batch = []

        # logging
        idx = idx + 1
        if verbose_logging:
            print(f'{idx}/{len(HTS_CODES)} -> HTS Code: {hts_code}; Requests: {len(summaries)}')
        for summary in summaries:
            details = erc.get_request_details(summary[0], summary)
            typed_details = dynamo.typify_value(details)
            batch.append(typed_details)

            if len(batch) == 20:
                write_batch(batch, dynamo, 'exclusion_requests', 'ID', verbose_logging=verbose_logging)
                batch.clear()

        if len(batch) > 0:
            write_batch(batch, dynamo, 'exclusion_requests', 'ID', verbose_logging=verbose_logging)
        pass

    idx = 0
    # PROCESS OBJECTION FILINGS
    dynamo.with_table('objection_filings', [('S', 'ID')], 'ID')
    objection_filings = erc.get_objection_filings()

    batch = []
    
    print('Extracting and loading objection filings...')
    filing_iterator = objection_filings if verbose_logging else tqdm(objection_filings)
    for filing in filing_iterator:
        # logging
        idx = idx + 1
        if verbose_logging:
            print(f'{idx}/{len(objection_filings)} -> Filing ID: {filing["id"]}')

        details = erc.get_objection_details(filing['id'], filing)
        typed_details = dynamo.typify_value(details)
        batch.append(typed_details)

        if len(batch) == 20:
            write_batch(batch, dynamo, 'objection_filings', 'id', verbose_logging=verbose_logging)
            batch.clear()
        pass

    if len(batch) > 0:
            write_batch(batch, dynamo, 'objection_filings', 'id', verbose_logging=verbose_logging)

__main__()