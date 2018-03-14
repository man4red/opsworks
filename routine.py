#!/usr/bin/env python
__author__ = "Egor Egorenkov"
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Egor Egorenkov"
__email__ = "it.manfred@gmail.com"
__status__ = "Development"

import socket
import requests
import boto3
import coloredlogs, logging
from datetime import datetime, timedelta
from contextlib import closing
from botocore.exceptions import ClientError, ParamValidationError
from operator import itemgetter

# EC2 Object
ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

# SETUP LOGGER
FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, filename='routine.log',level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level='INFO', logger=logger, fmt=FORMAT)

# VARIABLES
http_valid_codes = [200, 201, 202, 203, 204, 205, 206, 300, 301, 302, 303, 304, 307, 308]

# FUNCTIONS
def get_http_status_code(host, port = 80, max_redirects = 3):
    """Get HTTP status code
    :param host: host
    :param port: port 80|443, default = 80
    :param max_redirects: session max redirects default=3
    :return: Bool|Status code
    """
    if port == 443:
        prefix = 'https://'
    else:
        logger.debug('Only 80 and 443 port is accepted. Let''s try 80 by default...')
        prefix = 'http://'

    try:
        session = requests.Session()
        session.max_redirects = 3

        r = session.head(prefix + host, timeout=(2.34, 5.67), allow_redirects=True)
        try:
            logger.debug("closing connection")
            r.connection.close()
        except:
            pass
        return r.status_code

    except requests.ConnectionError:
        return False
    return False


def check_socket(host, port, timeout = 1):
    """Check socket
    :param host: host
    :param port: port
    :param timeout: connection timeout in seconds default = 1
    :return boolean
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)

    with closing(s) as sock:
        if sock.connect_ex((host, port)) == 0:
            logger.info('Server %s is available on port %d', host, port)
            return True
        else:
            logger.error('Server %s is not available on port %d', host, port)
            return False


def get_instances(key, value, state = ['running']):
    """Filter the running instances by given tag(team)
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#instance
    :param key: A string which is the key for the tag
    :param value: A string which is the value of the tag, here its team name
    :param state: A string which is the state of instance, default = ['running']
    :return: A list of tuples which is the information of the running instances
            example : [{'instance_name': 'name', 'instance_fqdn': 'fqdn', 'instance_id': 'i-xxxxx', 'instance.state': 'running', 'instance_type': 't2.micro'}]
    """
    results = []
    fltr = [{'Name': 'tag:' + key, 'Values': value}, {'Name': 'instance-state-name', 'Values': state}]
    try:
        instances = ec2_resource.instances.filter(
            Filters=fltr)
    except ClientError as e:
        logger.error("Unexpected error: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)
    
    for instance in instances:
        tags = instance.tags
        instance_name = None
        instance_fqdn = None
        for tag in tags:
            if tag['Key'] == 'Name':
                instance_name = tag['Value']
            if tag['Key'] == 'FQDN':
                instance_fqdn = tag['Value']
        results.append({
            'instance_name': instance_name,
            'instance_fqdn': instance_fqdn,
            'instance_id': instance.id,
            'instance_type': instance.instance_type,
            'instance_state': instance.state['Name']
        })
    return results

def get_owned_images(key, value):
    """Get owned images (filter by key, value)
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.describe_images
    :param key: key
    :param value: value
    :return: ["Images"] response
    """
    images = ec2_client.describe_images(
        Owners=['self'],
        Filters=[
            {
                'Name': key,
                'Values': [
                    value
                ]
            },
        ]
    )["Images"]
    return images

# Create an AMI of the stopped EC2 instance and add a descriptive tag based on the EC2 name along with the current date
def create_ami_and_add_tag(instance_id, instance_name):
    """Create AMI and add tag
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.create_image
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.create_tags
    :param instance_id: InstanceId
    :param instance_name: InstanceName
    """
    create_time = datetime.now()
    create_fmt = create_time.strftime('%Y-%m-%d-%H-%M-%S')
    name = instance_name + " - " + create_fmt
    description = "Autocreated image: " + name
    tag = None
    image_id = False
    
    image_id = ec2_client.create_image(
        InstanceId=instance_id,
        Name="" + name + "",
        Description="" + description + "",
        NoReboot=True,
        DryRun=False)['ImageId']

    logger.info("Waiting for image %s to be created...", image_id)
    waiter = ec2_client.get_waiter('image_available')
    waiter.wait(ImageIds=[image_id])
    
    logger.info("Image %s created", image_id)

    if image_id:
        ec2_client.create_tags(
            Resources=[
                image_id
            ],
            Tags=[
                {'Key': 'Name', 'Value': 'EgorAMI'},
            ]
        )
    else:
        logger.error('No AMI id')
        return False

    logger.info('Created AMI for instance %s (%s) with tags', instance_id, instance_name)
    return True

def terminate_instance(instance_id):
    """Terminate instances
        @TODO: return dict|objects
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.terminate_instances
    :param instance_id: InstanceId
    :return: boolean
    """
    try:
        logger.info("Terminating old instance %s", instance_id)

        ec2_client.terminate_instances(
            InstanceIds=[instance_id],
            DryRun=False)
            
        logger.info("Waiting while instance %s to be terminated...", instance_id)
        waiter = ec2_client.get_waiter('instance_terminated')
        waiter.wait(InstanceIds=[instance_id])
            
        logger.info('Instance %s is terminated', instance_id)
    except ClientError as e:
        logger.error(e)
        return False
    return True

def clenup_old_ami(days = 7):
    """Delete old AMI
        @TODO: return dict|objects
        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.deregister_image
    :param days: days count default = 7
    :return: boolean | dict
    """
    logger.warn('Cleanup AMI older than %d', days)

    results = []
    utc = datetime.utcnow() 

    for image in get_owned_images('name', 'egor*'):
        image_id = image['ImageId']
        image_name = image['Name']
        image_created = image['CreationDate']
        image_state = image['State']
        action = None
        
        #testdatetime = datetime.datetime.strptime(image_created, "%Y-%m-%dT%H:%M:%S.%fZ")
        utc_image_created = datetime.strptime(image_created[:-5], "%Y-%m-%dT%H:%M:%S")
        date_N_days_ago = utc - timedelta(days=days)
        
        if utc_image_created < date_N_days_ago:
            try:
                ec2_client.deregister_image(
                    ImageId=image_id,
                    DryRun=False)
                action = 'deregistered'
            except ClientError as e:
                logger.error(e)
                return False
            
        else:
            logger.info("image %s (%s) is newer %d days", image_id, image_name, days)
                
        results.append({
            'image_id': image_id,
            'image_name': image_name,
            'image_created': image_created,
            'image_state': image_state,
            'action': action
        })
    
    return results

    
def format_as_table(data,
                    keys,
                    header=None,
                    sort_by_key=None,
                    sort_order_reverse=False):
    """Takes a list of dictionaries, formats the data, and returns
    the formatted data as a text table.
    https://www.calazan.com/python-function-for-displaying-a-list-of-dictionaries-in-table-format/

    Required Parameters:
        data - Data to process (list of dictionaries). (Type: List)
        keys - List of keys in the dictionary. (Type: List)

    Optional Parameters:
        header - The table header. (Type: List)
        sort_by_key - The key to sort by. (Type: String)
        sort_order_reverse - Default sort order is ascending, if
            True sort order will change to descending. (Type: Boolean)
    """
    # Sort the data if a sort key is specified (default sort order
    # is ascending)
    if sort_by_key:
        data = sorted(data,
                      key=itemgetter(sort_by_key),
                      reverse=sort_order_reverse)

    # If header is not empty, add header to data
    if header:
        # Get the length of each header and create a divider based
        # on that length
        header_divider = []
        for name in header:
            header_divider.append('-' * len(name))

        # Create a list of dictionary from the keys and the header and
        # insert it at the beginning of the list. Do the same for the
        # divider and insert below the header.
        header_divider = dict(zip(keys, header_divider))
        data.insert(0, header_divider)
        header = dict(zip(keys, header))
        data.insert(0, header)

    column_widths = []
    for key in keys:
        column_widths.append(max(len(str(column[key])) for column in data))

    # Create a tuple pair of key and the associated column width for it
    key_width_pair = zip(keys, column_widths)

    format = ('%-*s ' * len(keys)).strip() + '\n'
    formatted_data = ''
    for element in data:
        data_to_format = []
        # Create a tuple that will be used for the formatting in
        # width, value format
        for pair in key_width_pair:
            data_to_format.append(pair[1])
            data_to_format.append(element[pair[0]])
        formatted_data += format % tuple(data_to_format)
    return formatted_data
    
# MAIN
def main():
    logger.info('Started')
    instances = get_instances('Name', ['egor*'], ['*'])

    results = []
    for instance in instances:
        instance_name = instance['instance_name']
        instance_fqdn = instance['instance_fqdn']
        instance_id = instance['instance_id']
        instance_state = instance['instance_state']

        status_code = None
        is_socket_open = False

        # Check socket
        is_socket_open = check_socket(instance_fqdn, 80)

        # Check status code
        if is_socket_open:
            status_code = get_http_status_code(instance_fqdn)
            if status_code:
                if status_code in http_valid_codes:
                    logger.info('Valid status code %d received from host %s', status_code, instance_fqdn)
                else:
                    logger.info('Status code %d is not in valid list for host %s', status_code, instance_fqdn)
            else:
                logger.warn('Request check failed for host %s', instance_fqdn)
        else:
            logger.warn('Request check skipped for host %s (probably socket is closed)', instance_fqdn)
        
        results.append({
            'instance_name': instance_name,
            'instance_fqdn': instance_fqdn,
            'instance_id': instance_id,
            'is_socket_open': is_socket_open,
            'status_code': status_code,
            'instance_state': instance_state
        })

    for result in results:
        # find stopped instances
        if not result['is_socket_open'] and result['status_code'] not in http_valid_codes and result['instance_state'] == 'stopped':
            # create ami
            create_ami_result = create_ami_and_add_tag(result['instance_id'],result['instance_name'])
            # terminate instance
            terminate_result = terminate_instance(result['instance_id'])
            # Change state
            if terminate_result:
                result['instance_state'] = 'terminated'

    clenup_result = clenup_old_ami(7)

    # OUTPUT
    header = ['instance_name', 'instance_fqdn', 'instance_id', 'is_socket_open', 'status_code', 'instance_state']
    keys = ['instance_name', 'instance_fqdn', 'instance_id', 'is_socket_open', 'status_code', 'instance_state']
    sort_by_key = 'instance_fqdn'
    sort_order_reverse = False

    logger.info("\n" + format_as_table(results,
                          keys,
                          header,
                          sort_by_key,
                          sort_order_reverse))
    
    logger.info('Finished')

if __name__ == '__main__':
    main()
