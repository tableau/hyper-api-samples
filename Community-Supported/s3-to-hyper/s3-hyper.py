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


from tableauhyperapi import *
from tableau_tools import *
from tableau_tools.tableau_documents import *
import tableauserverclient as TSC
import os, json, sys, glob, csv, fnmatch, \
     boto3, botocore, datetime


def get_csvs(name_format, header_file, bucket_name, profile_name):
    '''Uses boto3 to find matching filenames and download them locally.'''

    print(f"Downloading files matching {name_format} and {header_file}.")
    
    # Starts boto3 session based on profile name.
    # Will typically look for credential files /Users/[you]/.aws/credentials for OSX
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')
    
    # Creates list of all objects in a given bucket.
    try:
        filenames = s3.list_objects(Bucket=bucket_name)['Contents']
    except botocore.exceptions.ClientError as e:
        sys.exit(e)

    # Downloads matching pattern and header file.
    for filename in s3.list_objects(Bucket=bucket_name)['Contents']:
        if fnmatch.fnmatch(filename['Key'], header_file):
            print(f"Downloading {filename['Key']}...")
            s3.download_file(bucket_name, filename['Key'], filename['Key'])
        elif fnmatch.fnmatch(filename['Key'], name_format):
            print(f"Downloading {filename['Key']}...")
            s3.download_file(bucket_name, filename['Key'], filename['Key'])


def cleanup(name_format, header_file):
    '''Removes files matching the naming pattern and the header file in the local dir.'''
    pass
    datacsvs = glob.glob(name_format)
    headercsv = glob.glob(header_file)
    all_csvs = datacsvs + headercsv
    print(f"Running cleanup: Removing Files matching {name_format} and {header_file}.")
    for csv in all_csvs:
        os.remove(csv)


def create_initial_hyper(header_file, hyper_name, name_format, table_name, contains_header):
    '''Creates a table based on the header file.'''

    print(f'Creating table "{table_name}" with columns defined in {header_file} in {hyper_name}.')
    sql_mapping = get_sql_types()
    
    # Creates lists based on header_file.
    try:
        with open(header_file, mode='r', encoding='utf-8-sig') as header:
            header_reader = csv.reader(header, delimiter=',')
            header_list = list(header_reader)
    except Exception as e:
        sys.exit(e)

    # Creates table and temp table
    table = TableDefinition(
        table_name=TableName(table_name)
    )
    temp_table = TableDefinition(
        table_name=TableName('temp'),
        persistence=TEMPORARY
    )

    # Adds columns, with column name and type specified from config file.
    for col, col_type in zip(header_list[0], header_list[1]):
        sql_type = sql_mapping[col_type.lower()]
        column = TableDefinition.Column(col,sql_type)
        table.add_column(column)
        temp_table.add_column(column)
    
    column = TableDefinition.Column('File Path',SqlType.text())
    table.add_column(column)
    
    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.   
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        
        # Creates new Hyper file "[hyper_name].hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(table)
            connection.catalog.create_table(temp_table)
            print(f"Empty Hyper File with table: {table.table_name} created.")

            # Inserts data into table via add_to_hyper() method.
            add_to_hyper(hyper, connection, hyper_name, table, temp_table, name_format, contains_header)
            print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")    


def add_to_hyper(hyper, connection, hyper_name, table, temp_table, name_format, contains_header):
    '''Uses the Hyper API to build and insert data into Tableau Extract.'''

   # Finds CSVs based on name_format.
    for csvpath in glob.glob(os.getcwd()+'/'+name_format):
        # Load all rows into table_name from the CSV file.
        connection.execute_command(
            command=f"COPY {temp_table.table_name} FROM '{csvpath}' WITH "
            f"(format csv, NULL 'NULL', delimiter ',', header {contains_header})"
        )
        # Instert into persistent table with File Path col.
        connection.execute_command(
            command=f"INSERT INTO {table.table_name} SELECT *, '{csvpath}' FROM {temp_table.table_name}"
        )
        # Clear temp table.
        connection.execute_command(
            command=f"DELETE FROM {temp_table.table_name}"
        )
        print(f"Added data to table {table.table_name} from {csvpath}.")


def get_sql_types():
    '''Helper function to help programmatically map datatypes to Hyper SqlTypes.'''
    sql_mapping = {
        # Mapping according to Hyper SqlType name
        'big_int': SqlType.big_int(),
        'bool': SqlType.bool(),
        'bytes': SqlType.bytes(),
        'date': SqlType.date(),
        'double': SqlType.double(),
        'geography': SqlType.geography(),
        'int': SqlType.int(),
        'interval': SqlType.interval(),
        'json': SqlType.json(),
        'oid': SqlType.oid(),
        'small_int': SqlType.small_int(),
        'text': SqlType.text(),
        'timestamp': SqlType.timestamp(),
        'timestamp_tz': SqlType.timestamp_tz(),

        # Space for custom mappings
        'string': SqlType.text(),
        'varchar': SqlType.text(),
        'integer': SqlType.int(),
        'int64': SqlType.int()
    }   
    return sql_mapping


def load_config():
    '''Loads a config file in the current directory called config.json.'''

    # Opens the config file and loads as a dictionary.
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            return config
    except:
        message = 'Could not read config file.'
        print("Unexpected error: ", sys.exc_info()[0])
        sys.exit(message)


def swap_hyper(hyper_name, tdsx_name, logger_obj=None):
    '''Uses tableau_tools to open a local .tdsx file and replace the hyperfile.'''
    
    # Checks to see if TDSX exists, otherwise, as a one-time step, user will need to create using Desktop.
    if os.path.exists(tdsx_name):
        print("Found TDSX file.")
    else:
        message = "NOTE: Could not find existing TDSX file. Please use Desktop to create one from the new hyper " \
             "file or update the config file to find the correct TDSX. Refer to the documentation for more information."
        sys.exit(message)
    
    # Uses tableau_tools to replace the hyper file in the TDSX.
    try:
        local_tds = TableauFileManager.open(filename=tdsx_name, logger_obj=logger_obj)
    except TableauException as e:
        sys.exit(e)
    filenames = local_tds.get_filenames_in_package()
    for filename in filenames:
        if filename.find('.hyper') != -1:
            print("Overwritting Hyper in original TDSX...")
            local_tds.set_file_for_replacement(filename_in_package=filename,
                                            replacement_filname_on_disk=hyper_name)
            break
    
    # Overwrites the original TDSX file locally.
    tdsx_name_before_extension, tdsx_name_extension = os.path.splitext(tdsx_name)
    tdsx_updated_name = tdsx_name_before_extension + '_updated' + tdsx_name_extension
    local_tds.save_new_file(new_filename_no_extension=tdsx_updated_name)
    os.remove(tdsx_name)
    os.rename(tdsx_updated_name, tdsx_name)


def publish_to_server(site_name, server_address, project_name, tdsx_name, tableau_token_name, tableau_token):
    '''Publishes updated, local .tdsx to Tableau, overwriting the original file.'''
    
    # Creates the auth object based on the config file.
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=tableau_token_name, personal_access_token=tableau_token, site_id=site_name)
    server = TSC.Server(server_address)
    print(f"Signing into to site: {site_name}.")
    
    # Signs in and finds the specified project.
    with server.auth.sign_in(tableau_auth):
        #all_projects, pagination_item = server.projects.get()
        project_id = None
        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id
        if project_id == None:
            message = "Could not find project. Please update the config file."
            sys.exit(message)
        
        print(f"Publishing to {project_name}.")
        
        # Publishes the data source to Server/Online.
        overwrite_true = TSC.Server.PublishMode.Overwrite
        datasource = TSC.DatasourceItem(project_id)
        file_path = os.path.join(os.getcwd(), tdsx_name)
        datasource = server.datasources.publish(
            datasource, file_path, overwrite_true)
        print(f"Publishing of datasource '{tdsx_name}' complete.")


# Run
def main():
    '''Called to run the script.'''
    starttime = datetime.datetime.now()
    
    config = load_config() 
    get_csvs(config['name_format'], config['header_file'], config['bucket_name'], config['aws_cred_profile_name'])

    try:
        create_initial_hyper(config['header_file'], config['hyper_name'], config['name_format'],config['table_name'], config['contains_header'])
    except HyperException as e:
        sys.exit(e)
    finally:
        cleanup(config['name_format'], config['header_file'])

    swap_hyper(config['hyper_name'], config['tdsx_name'])
    
    publish_to_server(config['site_name'], config['server_address'], config['project_name'],
        config['tdsx_name'], config['tableau_token_name'], config['tableau_token'])
    
    print(f"Start time: {starttime} \nTotal time: {datetime.datetime.now()-starttime}")


if __name__ == '__main__':
    main()