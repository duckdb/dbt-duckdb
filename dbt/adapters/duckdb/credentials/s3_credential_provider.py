from dbt.logger import GLOBAL_LOGGER as logger

BOTO3_EXISTS = False

try:
    import boto3
    import boto3.session
    import botocore

    BOTO3_EXISTS = True
except ImportError:
    pass


class S3Credential:
    def __init__(self, access_key='', secret_key='', session_token='', region=''):
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.region = region

    @classmethod
    def from_settings(cls, settings):
        return cls(settings['s3_access_key_id'], settings['s3_secret_access_key'], settings['s3_session_token'],
                   settings['s3_region'])

    def to_dict(self) -> dict[str, str]:
        credentials = {}
        if self.access_key:
            credentials["s3_access_key_id"] = self.access_key
        if self.secret_key:
            credentials["s3_secret_access_key"] = self.secret_key
        if self.session_token:
            credentials["s3_session_token"] = self.session_token
        if self.region:
            credentials["s3_region"] = self.region
        return credentials


def _check_credentials(session):
    sts = session.client('sts')
    sts.get_caller_identity()


def _get_credentials_from_context(provider):
    if provider == '':
        return {}
    elif provider == 'aws':
        return _get_aws_credentials_from_context()
    else:
        raise RuntimeError(f'Unsupported credential provider specified {provider}')


def _get_aws_credentials_from_context() -> S3Credential:
    if BOTO3_EXISTS:
        logger.debug('Loading credentials from aws session')
        session = boto3.session.Session()
        _check_credentials(session)
        logger.info('Loaded AWS credentials from context')
        return S3Credential(session.get_credentials().access_key,
                            session.get_credentials().secret_key,
                            session.get_credentials().token,
                            session.region_name)
    raise RuntimeError('Boto3 not installed, unable to load aws credentials from context')


def get_s3_credentials(credentials) -> S3Credential:
    if credentials.settings:
        logger.debug('Using static credentials from settings')
        return S3Credential.from_settings(credentials.settings)
    elif credentials.use_credential_provider != '':
        logger.debug('Using credentials from context')
        return _get_credentials_from_context(credentials.use_credential_provider)
    else:
        logger.debug('No S3 credentials specified')
        return S3Credential()
