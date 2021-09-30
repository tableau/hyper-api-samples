# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------

from pathlib import Path 
import xml.etree.ElementTree as ET
import requests, os, math, json, random, getpass, sys, time, datetime

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException


# The namespace for the REST API is 'http://tableausoftware.com/api'
xmlns = {'t': 'http://tableau.com/api'}

# The REST API version we're using
VERSION = '3.10'

# The following packages are used to build a multi-part/mixed request.
# They are contained in the 'requests' library
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

# For when a workbook is over 64MB, break it into 5MB(standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5  # 5MB


class ApiCallError(Exception):
    pass


class UserDefinedFieldError(Exception):
    pass


def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.

    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')


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
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.
    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(server, username, password, site):
    """
    Signs in to the server specified with the given credentials
    'server'   specified server address
    'username' is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format(VERSION)

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)
    

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ET.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    return token, site_id


def sign_out(server, auth_token):
    """
    Destroys the active session and invalidates authentication token.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(VERSION)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 204)
    return


def start_upload_session(server, auth_token, site_id):
    """
    Creates a POST request that initiates a file upload session.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    Returns a session ID that is used by subsequent functions to identify the upload session.
    """
    url = server + "/api/{0}/sites/{1}/fileUploads".format(VERSION, site_id)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 201)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    return xml_response.find('t:fileUpload', namespaces=xmlns).get('uploadSessionId')


def get_default_project_id(server, auth_token, site_id):
    """
    Returns the project ID for the 'default' project on the Tableau server.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    """
    page_num, page_size = 1, 100  # Default paginating values

    # Builds the request
    url = server + "/api/{0}/sites/{1}/projects".format(VERSION, site_id)
    paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)
    server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))

    # Used to determine if more requests are required to find all projects on server
    total_projects = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
    max_page = int(math.ceil(total_projects / page_size))

    projects = xml_response.findall('.//t:project', namespaces=xmlns)

    # Continue querying if more projects exist on the server
    for page in range(2, max_page + 1):
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page)
        server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token})
        _check_status(server_response, 200)
        xml_response = ET.fromstring(_encode_for_display(server_response.text))
        projects.extend(xml_response.findall('.//t:project', namespaces=xmlns))

    # Look through all projects to find the 'default' one
    for project in projects:
        if project.get('name') == 'default' or project.get('name') == 'Default':
            return project.get('id')
    raise LookupError("Project named 'default' was not found on server")


def upload_file(file_path, server, auth_token, site_id):
    file = os.path.basename(file_path)
    filename, file_extension = file.split('.', 1)

    # print("\n3. Publishing '{0}' in {1}MB chunks (workbook over 64MB)".format(file, CHUNK_SIZE / 1024000))
    # Initiates an upload session
    uploadID = start_upload_session(server, auth_token, site_id)

    # URL for PUT request to append chunks for publishing
    put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(VERSION, site_id, uploadID)

    # Read the contents of the file in chunks of 100KB
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),
                                                     'tableau_file': ('file', data, 'application/octet-stream')})
            server_response = requests.put(put_url, data=payload,
                                           headers={'x-tableau-auth': auth_token, "content-type": content_type})
            _check_status(server_response, 200)
    return uploadID


def get_data_source_by_name(server, auth_token, site_id, ds_name):
    query_url = server + "/api/{0}/sites/{1}/datasources".format(VERSION, site_id)
    query_url += "?filter=name:eq:{0}".format(ds_name)
    server_response = requests.get(query_url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    datasources = xml_response.findall('.//t:datasource', namespaces=xmlns)
    if len(datasources) == 0:
        return None
    else:
        return datasources[0].get('id')


def main():
    # Create TableDefinition for payload. This must match the schema of the published .hyper file.
   
    # # # # # # # # # # # # # # # # # # # # # # # #
    #   Ensure this hyper file perfectly matches  #
    #       the columns/schema of the target      #
    # # # # # # # # # # # # # # # # # # # # # # # #

    schema = TableDefinition(TableName('Extract', 'Extract'), [
        TableDefinition.Column('Name', SqlType.text()),
        TableDefinition.Column('Symbol', SqlType.text()),
        TableDefinition.Column('Price', SqlType.double()),
        TableDefinition.Column('Volume', SqlType.double()),
        TableDefinition.Column('Market Cap', SqlType.double()),
        TableDefinition.Column('Date Time', SqlType.timestamp())
    ])


    # Set up environmental variables for publishing
    # These will be set from the AWS Environmental Variables config
    ds_name = os.environ['DATASOURCE'] # ex. 'CryptoPrices'
    server = os.environ['SERVER'] # ex. 'https://10ax.online.tableau.com/'
    username = os.environ['USERNAME'] # ex. 'user@gmail.com'
    password = os.environ['PASSWORD']
    site = os.environ['SITE'] # ex. 'sales'


    # Sign into Tableau and get the Project ID
    auth_token, site_id = sign_in(server, username, password, site)
    project_id = get_default_project_id(server, auth_token, site_id)


    # Grab datasource ID
    ds_id = get_data_source_by_name(server, auth_token, site_id, ds_name)


    # Set the .hyper file name and generate request ID
    hyper_file = "changeset.hyper"
    request_id = random.randrange(1000000)


    # # # # # # # # # # # # # # # # # # # # #
    #  Below is where you will modify the   #
    # sample to fit your specific use case. #
    # # # # # # # # # # # # # # # # # # # # #

    # Get data from API (or wherever!)
    url = "https://api.coinstats.app/public/v1/coins?skip=0&limit=5&currency=USD"
    response = requests.get(url)
    

    # Parse data & append to nested list
    data = json.loads(response.text)
    new_data = []
    dt = datetime.datetime.now()
    
    for coin in data['coins']:
        row = [coin['name'], coin['symbol'], coin['price'], coin['volume'], coin['marketCap'], dt]
        new_data.append(row)



    # Create changeset .hyper file
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, hyper_file, CreateMode.CREATE_IF_NOT_EXISTS) as connection:
            # Create a schema called 'Extract'
            connection.catalog.create_schema('Extract')
            
            # Create a table called 'new rows' for the rows to be inserted, and insert the data
            table_def = TableDefinition(TableName('Extract', 'new_rows'), schema.columns)
            connection.catalog.create_table(table_def)
            
            with Inserter(connection, table_def) as inserter:
                for d in new_data:
                    inserter.add_row(d)
                inserter.execute()
            

    # Upload new data to Tableau
    file_upload_id = upload_file(hyper_file, server, auth_token, site_id)

    # Use our change-set to update our data source via 'actions'
    json_request = {"actions": [
        # INSERT latest prices from 'new_rows' to 'Extract'
        {"action": "insert", 
            "source-schema":"Extract", "source-table": "new_rows",
            "target-schema": "Extract", "target-table": "Extract"}
    ]}

    # Create url for PATCH request
    patch_url = server + "/api/{0}/sites/{1}/datasources/{2}/data?uploadSessionId={3}" \
        .format(VERSION, site_id, ds_id, file_upload_id)
    
    # Send it up!
    server_response = requests.patch(patch_url, data=json.dumps(json_request), headers={'x-tableau-auth': auth_token,
                                                                                        'RequestID': str(request_id),
                                                                                        'content-type': 'application/json',
                                                                                        'Accept': 'application/xml'})
    _check_status(server_response, 202)


    # Sign out and clean up tmp files
    os.remove(hyper_file)
    sign_out(server, auth_token)