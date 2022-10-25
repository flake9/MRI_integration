import requests
import logging
import base64

from MRI_integration.MRI_integration_consts import *

class MRIIntegration:

    def _make_rest_call(self, url=None, params=None, headers=None, data=None, method="get"):

        try:
            request_func = getattr(requests, method)
        except Exception as e:
            error_message = str(e)
            return False, error_message

        try:
            response = request_func(url, params=params, headers=headers, data=data)
        except Exception as e:
            error_message = str(e)
            return False, error_message

        return self._process_response(response)

    def _process_response(self, r):

        # Process a json response
        if 'json' in r.headers.get('Content-Type', ''):
            return self._process_json_response(r)

        message = "Can't process response from server. Status Code: {0} Data from server: {1}".format(
                r.status_code, r.text.replace('{', '{{').replace('}', '}}'))

        return False, message

    def _process_json_response(self, r):

        # Try a json parse
        try:
            resp_json = r.json()
        except Exception as e:
            logger.debug('Cannot parse JSON')
            return False, "Unable to parse response as JSON"

        if (200 <= r.status_code < 205):
            return True, resp_json

        error_info = resp_json if type(resp_json) is str else resp_json.get('error', {})
        try:
            if error_info.get('code') and error_info.get('message') and type(resp_json):
                error_details = {
                    'message': error_info.get('code'),
                    'detail': error_info.get('message')
                }
                return False, "Error from server, Status Code: {0} data returned: {1}".format(r.status_code, error_details)
            else:
                return False, "Error from server, Status Code: {0} data returned: {1}".format(r.status_code, r.text.replace('{', '{{').replace('}', '}}'))
        except:
            return False, "Error from server, Status Code: {0} data returned: {1}".format(r.status_code, r.text.replace('{', '{{').replace('}', '}}'))


if __name__ == '__main__':

    logging.basicConfig(filename="mri.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

    # Creating an object
    logger = logging.getLogger()
    
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    connector = MRIIntegration()

    auth_string_username = '{}/{}/{}/{}'.format(CLIENT_ID, DATABASE_NAME, API_USERNAME, API_DEVELOPER_KEY)

    auth_header = base64.b64encode('{}:{}'.format(auth_string_username, API_PASSWORD).encode()).decode()

    get_properties_headers = {
        'Authorization': 'Basic {}'.format(auth_header)
    }

    bank_map_list = []
    bank_map_dict = {}
    bank_map_endpoint = "{}{}".format(MRI_BASE_URL, '/MRIAPIServices/api.asp?$api=MRI_S-PMAP_Bank&$format=json')
    next_link = None

    while True:
        if next_link:
            ret_val, bank_map_response = connector._make_rest_call(url=next_link.replace('%24', '$'), headers=get_properties_headers, method="get")
        else:
            ret_val, bank_map_response = connector._make_rest_call(url=bank_map_endpoint, headers=get_properties_headers, method="get")

        if not ret_val:
            logger.debug("Error occured while fetching bank mapping from MRI. Error {}".format(bank_map_response))
            continue

        if bank_map_response.get("value"):
            bank_map_list.extend(bank_map_response.get("value", []))

        next_link = bank_map_response.get('nextLink', None)

        if not next_link:
            break

    bank_map_dict = { bank['BankID']:bank['BankName'] for bank in bank_map_list }
    logger.debug("bank map {}".format(bank_map_dict))

    get_properties_params = {
        '$top': PAGE_SIZE
    }

    properties_list = []
    prop_details = {}

    get_properties_endpoint = "{}{}".format(MRI_BASE_URL, '/MRIAPIServices/api.asp?$api=MRI_S-PMRM_PropertyIDByNameOrAddress&$format=json')

    next_link = None

    while True:
        if next_link:
            ret_val, properties_response = connector._make_rest_call(url=next_link.replace('%24', '$'), headers=get_properties_headers, params=get_properties_params, method="get")
        else:
            ret_val, properties_response = connector._make_rest_call(url=get_properties_endpoint, headers=get_properties_headers, params=get_properties_params, method="get")

        if not ret_val:
            logger.debug("Error occured while fetching properties from MRI. Error {}".format(properties_response))

        if properties_response.get("value"):
            properties_list.extend(properties_response.get("value", []))

        next_link = properties_response.get('nextLink', None)

        if not next_link:
            break
    
    for a_property in properties_list:
        entity_id = a_property.get('EntityId')
        prop_details['PropertyName'] = a_property.get('PropertyName', '')
        prop_details['ShortName'] = ''
        prop_details['Email'] = a_property.get('Address3', '')
        prop_details['ManagerName'] = a_property.get('ManagerName', '')
        prop_details['PropertyType'] = ''
        prop_details['TaxID'] = '31-5432654'
        prop_details['PropertyID'] = a_property.get('PropertyID', '')


        get_prop_bank = "{}{}".format(MRI_BASE_URL, '/MRIAPIServices/api.asp?$api=MRI_S-PMAP_BankAccountMapping&$format=json')
        get_prop_bank_param = {
            'EntityId': entity_id
        }
        ret_val, prop_bank = connector._make_rest_call(url=get_prop_bank, headers=get_properties_headers, params=get_prop_bank_param, method="get")
        prop_bank = prop_bank.get('value', [])

        prop_bank_set = set()
        for bank in prop_bank:
            prop_bank_set.add(bank['BankID'])

        bank_list = [bank_map_dict[bank] for bank in list(prop_bank_set)]

        prop_details['Bank'] = bank_list,
        Address = {
            'Address': a_property.get('Address1', ''),
            'Street': a_property.get('Address2', ''),
            'City': a_property.get('City', ''),
            'State': a_property.get('State', ''),
            'PostalCode': a_property.get('ZipCode', '')
        }
        prop_details['Address'] = Address
        prop_details['PhoneNumbers'] = [str(a_property.get('PhoneNumber', ''))]

        logger.debug("prop details {}".format(prop_details))



    # Unit part start here
    properties_ids = [prop['PropertyID'] for prop in properties_list]

    units_list = []

    for property_id in properties_ids:

        get_units_headers = {
        'Authorization': 'Basic {}'.format(auth_header)
        }

        get_units_params = {
            '$top': PAGE_SIZE
        }
        logger.debug(property_id)
        get_units_endpoint = "{}{}".format(MRI_BASE_URL, '/MRIAPIServices/api.asp?$api=MRI_S-PMRM_UnitVacancyInformation&$format=json&PROPERTYID={}'.format(property_id))

        next_link = None

        while True:
            if next_link:
                ret_val, units_response = connector._make_rest_call(url=next_link.replace('%24', '$'), headers=get_units_headers, params=get_units_params, method="get")
            else:
                ret_val, units_response = connector._make_rest_call(url=get_units_endpoint, headers=get_units_headers, params=get_units_params, method="get")

            if not ret_val:
                logger.debug("Error occured while fetching properties from MRI. Error {}".format(units_response))
                continue

            if units_response.get("value"):
                units_list.extend(units_response.get("value", []))

            next_link = units_response.get('nextLink', None)

            if not next_link:
                break
        
        logger.debug("units list {}".format(units_list))