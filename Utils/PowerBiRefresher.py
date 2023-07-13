from Utils.Logger import get_logger
from os import environ
import requests as rq
import json

logger = get_logger(__name__)


def refresh():
    try:
        api_url = environ['POWERBI_REFRESH_API_URL']
        workspace = environ['POWERBI_REFRESH_WORKSPACE']
        dataset = environ['POWERBI_REFRESH_DATASET']

        api_url += '/RefreshNow'
        params = {
            'workspace': workspace,
            'dataset': dataset
        }

        logger.info('Sending request to PowerBI Refresh API')

        with rq.post(url=api_url, params=params) as resp:
            if resp.status_code == 200:
                message = json.loads(resp.text)['message']
                logger.info(message)
            elif resp.status_code == 400:
                message = json.loads(resp.text)['message']
                logger.error(message)
            elif resp.status_code == 404:
                logger.error('The API URL cannot be founded. Aborting refresh process...')
            else:
                raise Exception

        logger.info('Power BI was refreshed successfully')

    except KeyError as err:
        key = err.args[0]
        logger.error('The environment variable "{}" is required to refresh Power BI. Aborting process...'.format(key))
    except Exception:
        logger.error('An error occurs trying to refresh Power BI. Consult the API logs to details')
