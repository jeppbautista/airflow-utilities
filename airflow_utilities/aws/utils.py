from urllib.parse import urlparse
from airflow.exceptions import AirflowException


def remove_leading_slash_if_exists(str_with_slash: str) -> str:
    """
    A function that removes a leading forward slash to a string if it exists.

    Parameters
    ----------
    str_with_slash : str
        A string which may or may not contain a leading forward slash (/).

    Returns
    -------
    str
        The input string with the leading forward slash removed. If there was no leading forward slash then the same input string is returned.
    """
    if str_with_slash.startswith("/"):
        return str_with_slash[1:]
    else:
        return str_with_slash


def parse_bucket_and_path_from_uri(s3_path_uri: str) -> tuple[str, str]:
    """
    A function that accepts an s3 path in this format: ``s3://bucket/sample/prefix/path`` and parses it to split the
    s3 bucket from the prefix.

    Sample usage:
    ```
    print(parse_bucket_and_path_from_uri("s3://sample-bucket/sample/prefix/path"))
    ('sample-bucket', 'sample/prefix/path')
    ```

    Parameters
    -----------
    s3_path_uri : str
        A valid s3 path in the following format: ``s3://bucket/sample/prefix/path``.

    Returns
    --------
    tuple[str, str]:
        A tuple containing:

        - str: The bucket name of the given s3 path.
        - str: The prefix of the given s3 path.
    """
    parsed_url = urlparse(s3_path_uri)
    if parsed_url.scheme != 's3':
        raise AirflowException(
            f'Expected an s3:// url, instead got {parsed_url.scheme}://'
        )
    if parsed_url.netloc == '':
        raise AirflowException(
            f'Did not find bucket_name, expected s3://<bucket_name>/path (found {s3_path_uri})'
        )
    bucket_name = parsed_url.netloc
    bucket_path = remove_leading_slash_if_exists(parsed_url.path) # path doesn't need the leading slash

    return bucket_name, bucket_path