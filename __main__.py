from clients.dynamodb import DyanmoDBClient
from config import HTS_CODES
from clients.exclusion_requests import ExclusionRequestsClient
from tqdm import tqdm

def write_batch(batch, dynamo, to_table, id_field):
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
    erc = ExclusionRequestsClient()
    dynamo = DyanmoDBClient()
    dynamo.with_table('exclusion_requests', [('S', 'ID')], 'ID')
    print('Extracting and loading exclusion requests...')
    for hts_code in tqdm(HTS_CODES):
        summaries = erc.get_summaries(hts_code)
        batch = []
        for summary in summaries:
            details = erc.get_request_details(summary[0], summary)
            typed_details = dynamo.typify_value(details)
            batch.append(typed_details)

            if len(batch) == 20:
                write_batch(batch, dynamo, 'exclusion_requests', 'ID')
                batch.clear()

        if len(batch) > 0:
            write_batch(batch, dynamo, 'exclusion_requests', 'ID')
        pass

    dynamo.with_table('objection_filings', [('S', 'ID')], 'ID')
    objection_filings = erc.get_objection_filings()
    batch = []
    print('Extracting and loading objection filings...')
    for filing in tqdm(objection_filings):
        details = erc.get_objection_details(filing['id'], filing)
        typed_details = dynamo.typify_value(details)
        batch.append(typed_details)

        if len(batch) == 20:
            write_batch(batch, dynamo, 'objection_filings', 'id')
            batch.clear()
        pass

    if len(batch) > 0:
            write_batch(batch, dynamo, 'objection_filings', 'id')

__main__()