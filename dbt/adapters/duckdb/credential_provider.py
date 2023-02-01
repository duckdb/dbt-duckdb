from dbt.logger import GLOBAL_LOGGER as logger

BOTO3_EXISTS = False

try:
    import boto3
    import boto3.session
    import botocore

    BOTO3_EXISTS = True
except ImportError:
    pass


def check_credentials():
    sts = boto3.client('sts')
    try:
        sts.get_caller_identity()
        return True
    except botocore.exceptions.ClientError:
        return False


def get_credentials_from_context(provider):
    if provider == '':
        return {}
    elif provider == 'aws':
        return get_aws_credentials_from_context()
    else:
        raise RuntimeError(f'Unsupported provider specified {provider}')


def get_aws_credentials_from_context():
    if BOTO3_EXISTS:
        logger.debug('Loading credentials from aws session')
        session = boto3.session.Session()
        valid = check_credentials()
        if valid:
            logger.info('Loaded AWS credentials from context')
            return {"s3_access_key_id": session.get_credentials().access_key,
                    "s3_secret_access_key": session.get_credentials().secret_key,
                    "s3_session_token": session.get_credentials().token,
                    "s3_region": session.region_name, }
        else:
            raise RuntimeError('No valid AWS credentials could be loaded from context')
    logger.debug('No boto3 on classpath, cannot load aws credentials')
    return {}
