from urllib.error import HTTPError
from aiohttp import ClientSession
import asyncio
import json
from os import getenv

TOKEN = getenv('MDL_TOKEN')
URL = "https://webcampus.uws.edu/"
ENDPOINT = "webservice/rest/server.php"

class MdlWebServicesClient():
    """Make asynchronous API calls on Moodle Web Services REST API.
    
    Args:
        wsfunction (str): name of Moodle Web Service function
        param_key (str): name of required API argument
        param_values (list): list of `param_key` values to call asyncronously

     Attributes:
        wsfunction (str): name of Moodle Web Service function
        param_key (str): name of required API argument
        param_values (list): list of `param_key` values to call asyncronously
        errors (list): list of param_values that recieved error on call.
    """
    def __init__(self, wsfunction, param_key, param_values):
        self.param_key = param_key
        self.param_values = param_values
        self.wsfunction = wsfunction
        self.errors = []

    def rest_api_parameters(self, in_args, prefix='', out_dict=None):
        """Transform dictionary/array structure to a flat dictionary, with key names
        defining the structure.

        Example usage:
        >>> rest_api_parameters({'courses':[{'id':1,'name': 'course1'}]})
        {'courses[0][id]':1,
         'courses[0][name]':'course1'}"""
        if out_dict==None:
            out_dict = {}
        if not type(in_args) in (list, dict):
            out_dict[prefix] = in_args
            return out_dict
        if prefix == '':
            prefix = prefix + '{0}'
        else:
            prefix = prefix + '[{0}]'
        if type(in_args)==list:
            for idx, item in enumerate(in_args):
                self.rest_api_parameters(item, prefix.format(idx), out_dict)
        elif type(in_args)==dict:
            for key, item in in_args.items():
                self.rest_api_parameters(item, prefix.format(key), out_dict)
        return out_dict

    async def fetch(self, session, value):
        """Make single POST request to Moodle Web Services API
        within asynchronous ClientSession.
        """
        parameters = self.rest_api_parameters({self.param_key: value})
        parameters.update({"wstoken": TOKEN, 'moodlewsrestformat': 'json',
                           "wsfunction": self.wsfunction})
        try:
            response = await session.post(url=URL+ENDPOINT, data=parameters)
            response.raise_for_status()
            response_json = await response.json()
            # validate json
            return {self.param_key: value,
                    'results': response_json}
        except HTTPError as http_err:
            self.errors.append(value)
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            self.errors.append(value)
            print(f"An error ocurred: {err}")

    async def fetch_all(self):
        """Gather all asynchronously POST opperations in a ClientSession
        on Moodle WebServices API.
        """
        async with ClientSession() as session:
            tasks = [self.fetch(session, value)
                     for value in self.param_values]

            responses = await asyncio.gather(*tasks)

        return {response[self.param_key]: response['results']
                for response in responses}

class WebCampusRosters(MdlWebServicesClient):
	"""Class to make asyncronous calls to Moodle Web Service
	API function core_enrol_get_enrolled_users.

	Args:
        wsfunction (str): name of Moodle Web Service function
        param_key (str): name of required API argument
        param_values (list): list of `param_key` values to call asyncronously

     Attributes:
        wsfunction (str): name of Moodle Web Service function
        param_key (str): name of required API argument
        param_values (list): list of `param_key` values to call asyncronously
        errors (list): list of param_values that recieved error on call
	"""
    def __init__(self, param_values, param_key='courseid',
    	         wsfunction='core_enrol_get_enrolled_users'):
        super().__init__(wsfunction, param_key, param_values)

    async def get_rosters(self):
        """Wrapper to run get enrollment data from Moodle
        WebServices API asynchronously.
        """
        responses = await self.fetch_all()
        return {key: {'student': self.__flatten_roster(responses[key], roleid=5),
                      'auditingstudent': self.__flatten_roster(responses[key], roleid=14)}
                for key in responses}

    def __flatten_roster(self, response, roleid=5):
        """Helper function to return set of enrolled students from nested dictionary
        """
        return set(user.get('idnumber') for user in response
                   if user.get('idnumber') and self.__has_role(user.get('roles'), roleid))

    def __has_role(self, roles, roleid):
        """Helper function to determin whether an enrolled user in a nested dictionary
        has a role
        """
        return any(map(lambda role: True if role.get('roleid') == roleid
                       else False, roles))
