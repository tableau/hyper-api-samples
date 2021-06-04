""" Tableau RestAPI Helpers

Tableau Community supported Hyper API sample

The helper functions in this module are only used when REST API functionality
is not yet available in the standard tableauserverclient libraries. (e.g. PATCH
for update/upsert. Once these get added to the standard client libraries then
this module will be refactored out.
NOTE: Most of these utility functions are copied from dataupdate_example.py which
was included with the prerelease for Data Update REST API Extensions.

-----------------------------------------------------------------------------

This file is the copyrighted property of Tableau Software and is protected
by registered patents and other applicable U.S. and international laws and
regulations.

You may adapt this file and modify it to fit into your context and use it
as a template to start your own projects.

-----------------------------------------------------------------------------
"""

import logging
import functools

import json
import requests  # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET  # Contains methods used to build and parse XML
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

import os
import uuid


# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
xmlns = {"t": "http://tableau.com/api"}

# The REST API version we're using
VERSION = "3.10"

# For when a workbook is over 64MB, break it into 5MB(standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5  # 5MB

logger = logging.getLogger("hyper_samples.restapi_helpers")


class ApiCallError(Exception):
    pass


def debug(func):
    """Log the function arguments and return value"""

    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]  # 1
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
        signature = ", ".join(args_repr + kwargs_repr)  # 3
        logger.debug(f"Calling {func.__name__}({signature})")

        value = func(*args, **kwargs)

        logger.debug(f"{func.__name__!r} returned {value!r}")  # 4

        return value

    return wrapper_debug


def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.

    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode("ascii", errors="backslashreplace").decode("utf-8")


def _make_multipart(parts):
    """
    Creates one "chunk" for a multi-part upload
    'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).
    Returns the post body and the content type string.
    For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
    """
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = "".join(("multipart/mixed",) + content_type.partition(";")[1:])
    return post_body, content_type


@debug
def check_status(server_response, success_code):
    """
    Checks the server response for possible errors.

    server_response: the response received from the server
    success_code: the expected success code for the response

    Throws an ApiCallError exception if the API call fails.
    """
    logger.debug(
        "Checking for success_code {} from server_reponse: {}".format(
            success_code, server_response.text
        )
    )
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find("t:error", namespaces=xmlns)
        summary_element = parsed_response.find(".//t:summary", namespaces=xmlns)
        detail_element = parsed_response.find(".//t:detail", namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = (
            error_element.get("code", "unknown")
            if error_element is not None
            else "unknown code"
        )
        summary = (
            summary_element.text if summary_element is not None else "unknown summary"
        )
        detail = detail_element.text if detail_element is not None else "unknown detail"
        error_message = "{0}: {1} - {2}".format(code, summary, detail)
        logger.error(error_message)
        raise ApiCallError(error_message)
    return


@debug
def start_upload_session(server, auth_token, site_id):
    """
    Creates a POST request that initiates a file upload session.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    Returns a session ID that is used by subsequent functions to identify the upload session.
    """
    url = server + "/api/{0}/sites/{1}/fileUploads".format(VERSION, site_id)
    server_response = requests.post(url, headers={"x-tableau-auth": auth_token})
    check_status(server_response, 201)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    return xml_response.find("t:fileUpload", namespaces=xmlns).get("uploadSessionId")


@debug
def upload_file(file_path, server, auth_token, site_id):
    logger.info("Uploading {} to Tableau Server...".format(file_path))
    file = os.path.basename(file_path)
    filename, file_extension = file.split(".", 1)

    logger.info(
        "\n3. Publishing '{0}' in {1}MB chunks (workbook over 64MB)".format(
            file, CHUNK_SIZE / (1024 * 1024)
        )
    )
    # Initiates an upload session
    uploadID = start_upload_session(server, auth_token, site_id)

    # URL for PUT request to append chunks for publishing
    put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(
        VERSION, site_id, uploadID
    )

    # Read the contents of the file in chunks of 100KB
    with open(file_path, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            payload, content_type = _make_multipart(
                {
                    "request_payload": ("", "", "text/xml"),
                    "tableau_file": ("file", data, "application/octet-stream"),
                }
            )
            logger.debug("\tPublishing a chunk...")
            server_response = requests.put(
                put_url,
                data=payload,
                headers={"x-tableau-auth": auth_token, "content-type": content_type},
            )
            check_status(server_response, 200)
    logger.info("Upload completed.  Upload ID={}".format(uploadID))

    return uploadID


@debug
def patch_datasource(
    server, auth_token, site_id, datasource_id, file_upload_id, request_json
):
    """
    Submits a PATCH request against specified datasource
    returns Asynchronous Job ID

    'server'   specified server address
    'auth_token', 'site_id' from sign_in
    'datasource_id' Target Datasource on Tableau Server
    'file_upload_id' from upload_file
    'request_json' the data={} part of the PATCH call
    """

    # Generate request id using standard UUID module
    request_id = uuid.uuid4()

    patch_url = server + "/api/{0}/sites/{1}/datasources/{2}".format(
        VERSION, site_id, datasource_id
    )
    if file_upload_id is not None:
        patch_url += "/data"
        patch_url += "?uploadSessionId={0}".format(file_upload_id)

    logger.info(
        "Updating datasource {} on Tableau Server {}:{}".format(
            datasource_id, server, patch_url
        )
    )
    server_response = requests.patch(
        patch_url,
        data=json.dumps(request_json),
        headers={
            "x-tableau-auth": auth_token,
            "RequestID": str(request_id),
            "content-type": "application/json",
            "Accept": "application/xml",
        },
    )

    check_status(server_response, 202)
    # Get Asynchronous Job ID
    server_response = _encode_for_display(server_response.text)
    parsed_response = ET.fromstring(server_response)
    async_job_id = parsed_response.find("t:job", namespaces=xmlns).get("id")
    logger.info(f"Asynchronous Job ID:{async_job_id}")

    return async_job_id


def main():
    pass


if __name__ == "__main__":
    main()
