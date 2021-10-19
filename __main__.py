from clients.dynamodb import DyanmoDBClient
from config import HTS_CODES
from clients.exclusion_requests import ExclusionRequestsClient
import json

def write_batch(batch, dynamo):
    dynamo.client.batch_write_item(
        RequestItems={
            'exclusion_requests': [
                {
                    'PutRequest': {
                        'Item': {
                            'ID': document['M']['ID'],
                            'Details': document
                        }
                    }
                } for document in batch
            ]
        }
    )

def __main__():
    erc = ExclusionRequestsClient()
    erc.get_objection_filings()
    return
    #dynamo = DyanmoDBClient()
    #dynamo.with_table('exclusion_requests', [('S', 'ID')], 'ID')
    for hts_code in HTS_CODES:
        break
        summaries = erc.get_summaries(hts_code)
        batch = []
        for summary in summaries:
            details = erc.get_request_details(summary[0], summary)
            typed_details = dynamo.typify_value(details)
            batch.append(typed_details)

            if len(batch) == 10:
                write_batch(batch, dynamo)
                batch.clear()

        if len(batch) > 0:
            write_batch(batch, dynamo)
    objection_filings = erc.get_objection_filings()
    for filing in objection_filings:
        details = erc.get_objection_details(filing['id'], filing)
        print(details)
__main__()