BOTO3_EXISTS = False

try:
    import boto3
    BOTO3_EXISTS = True
except ImportError:
    pass


def get_credentials_from_context():
    if BOTO3_EXISTS:
        session = boto3.Session()
        return {"s3_access_key_id": session.get_credentials().access_key,
                "s3_secret_access_key": session.get_credentials().secret_key,
                "s3_session_token": session.get_credentials().token,
                "s3_region": session.region_name, }
    return {}
