"""Tasks for interacting with AWS Lambda"""

from typing import Any, Dict, ByteString, Optional
import json
from base64 import b64encode

from prefect import get_run_logger, task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_aws.credentials import AwsCredentials


def encode_lambda_context(custom=None, env=None, client=None):

    """
    Utility function for encoding Lambda context
    Args:
        - custom (dict, optional): key-value  pairs to pass to custom context
        - env (dict, optional): key-value pairs to pass to environment context
        - client (dict, optional): key-value pairs to pass to client context
    Returns:
        - json: base64 encoded json object
    """

    client_context = dict(custom=custom, env=env, client=client)
    json_context = json.dumps(client_context).encode("utf-8")
    return b64encode(json_context).decode("utf-8")

@task
async def lambda_invoke(
    function_name: str,
    aws_credentials: AwsCredentials,
    qualifier: Optional[str] = "current",
    invocation_type: Optional[str] = 'RequestResponse',
    client_context: Optional[ByteString] = encode_lambda_context(None),
    payload: Optional[Dict] = json.dumps(None),
    log_type: Optional[str] = 'Tail',
    **lambda_kwargs: Optional[Dict[str, Any]],
):
    """
    Invokes a Lambda function.
    Args:
        - function_name (str): the name of the Lambda funciton to invoke
        - invocation_type (str, optional): the invocation type of Lambda
            function, default is RequestResponse other options include
            Event and DryRun
        - log_type (str, optional): set to 'Tail' to include the execution
            log in the response
        - client_context (dict, optional): data to pass to the function in the
            context object, dict object will be transformed into base64 encoded
            json automatically
        - payload (bytes or seekable file-like object): the JSON provided to
            Lambda function as input
        - qualifier (str, optional): specify a version or alias to invoke a
            published version of the function, defaults to $LATEST
        - **lambda_kwargs (dict, optional): additional keyword arguments to pass to the
            boto `invoke_lambda` function

    Returns:
        The status code from the Lambda invocation

    Example:
        Invokes a Lambda function

        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.lambda_functions import lambda_invoke


        @flow
        def example_lambda_invoke_flow():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )
            invocation = lambda_invoke(
                "function_name",
                aws_credentials,
                qualifier,
                log_type,
                client_context,
                payload,
            )
            return invocation

        example_lambda_invoke_flow()
        ```

    """  # noqa
    logger = get_run_logger()
    logger.info("Preparing to invoke %s", function_name)
    lambda_kwargs = lambda_kwargs or {}
    lambda_client = aws_credentials.get_boto3_session().client("lambda")

    client_context = encode_lambda_context(client_context)

    response = await run_sync_in_worker_thread(
        lambda_client.invoke,
        FunctionName=function_name,
        InvocationType=invocation_type,
        Qualifier=qualifier,
        LogType=log_type,
        ClientContext=client_context,
        Payload=payload,
        **lambda_kwargs,
    )
    logger.info("Invoked lambda")
    if log_type == 'Tail':
        lambda_payload = response['Payload']
        lambda_text = lambda_payload.read()
        logger.info(lambda_text)
    return response['StatusCode']
