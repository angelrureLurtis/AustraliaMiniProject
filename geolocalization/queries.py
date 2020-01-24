"""
Simple module to perform geolocalization queries and retrieve their corresponding
indicators.

Functions started with underscore are not intended to be modified by the user as
they usually deal with how the API is structured.

General use:

    import queries

    queries.search_by_distance(location='Sydney',distance=100, distance_type='miles'
                                         include_indicators_with_assets=True, 
                                         folder_name='Any')
"""

import requests
import pandas as pd

def search_by_distance(location, distance, distance_type, 
                       include_indicators_with_assets=True, folder_name='Any',
                       indicator=None):
    """

    General high level function that searches the indicators inside a radius of 
    specific length from a source location.

    Args:

        location(str): The source point location. It is send to a geolocalization API
                       so it can take many forms like a city, and address.
        distance(int): The distance to draw the searching circle from the location.
        distance_type(str): Either 'km' for kilometers or 'miles'.
        include_indicators_with_assets(bool): If True only retrieves indicators with 
        an associated asset. If False only retrieve the ones without.
        folder_name(str): One of the available folder_names to look onto. Can be either:
                          'Any', 'Retail', 'Residential', 'Country Specific' or 'Office'.
    
    Returns:

        (dict): A JSON with the requested data.
    """
    
    if distance_type.lower() not in ['km', 'miles']:
        raise Exception('Invalid distance type. Please use either \'km\' or \'miles\'')
    
    settings = _retrieve_query_settings(simple=False)

    results = _search_by_distance(**settings, location=location, distance=distance, 
                                  distance_type=distance_type, folder_name=folder_name,
                                  include_indicators_with_assets=include_indicators_with_assets,
                                  indicator=indicator)
    return results

def indicators_given_asset(asset_name):
    
    settings = _retrieve_query_settings(ds='search')
    
    url = settings['url']
    path = settings['path']
    port = settings['port']
    
    results = requests.get(f'{url}:{port}/{path}', params={'asset':asset_name,
                                                         'model':'Actual',
                                                         'output_format':'json'})
    
    return results



def return_all_national_indicators(location='Sydney', parse_json=False):
    """
    
    General function to retrieve all the national indicators from a Country.

    Args:

        location(str): a city or location in the target Country. For the
                       time being it only works on Australia.

    Returns:

        dict: a Json with the indicators, their metadata and their values.
    """

    
    settings = _retrieve_query_settings()
    
    url, port, path = settings['url'], settings['port'], settings['path']
    
    data = {
        'location_name':location,
        'distance_type':'km',
        'only_nationals':True,
    }
    
    request = requests.post(f'{url}:{port}/{path}', json=data)
    
    results = {}

    json_out = request.json()

    for tag in json_out['responses']:
        for result in request.json()['responses'][tag]:
            results[tag] = {}
            sk = result['search_key']
            sk_parsed = _parse_search_key_info(sk)
            settings = _retrieve_query_settings(simple=True, add_method=False)
            result2 = _match_key_to_actual(**settings, sk=sk)
            results[tag] = sk_parsed
            results[tag].update(result2)

        if parse_json:
            
            if tag in results.keys():
                results[tag] = parse_inner_lists(results[tag], 
                                                ['actual_value', 'date'], 
                                                'responses', 'all')

    if parse_json:

        results = merge_keys(results, 'folder_name')

    return results

def parse_inner_lists(json_data, cols_from_nested, target_nested_column, to_keep_columns):
    """
    Parses JSON data that contains a nested column with a list of dicionaries. 
    This is inteded to make much easier the conversion to a pandas DataFrame.
    
    Args:
        json_data(dict): a dictionary containing one nested value.
        cols_from_nested(list): a list containing the values to extract from
                                the nested dictionaries.
        target_nested_column(str): the name of the value that contains a list
                                   of dicionaries.
        to_keep_columns(list or 'all'): either a list containing the name of 
                                        the unnested columns to keep or the
                                        string 'all' which means to keep them
                                        all. If list, do not include the value
                                        that has the nested values.
    
    Returns:
        
        A dictionary with the nested value unnested. Ideally to be feed 
        directly into a Pandas DataFrame.
    
    Usage example:
    
        parsed = parse_inner_lists(request.json(), ['actual_value', 'date'], 
                                   'responses', 'all')

        clean = pd.DataFrame(parsed)
        
    """
    
    results = []

    if to_keep_columns=='all':
        
        to_keep_columns = [col for col in json_data.keys() if col != target_nested_column]
        
    for response in json_data[target_nested_column]:
        
        r = {}

        for col in cols_from_nested:

            r.update({
                col:response[col],
            })

        for col in to_keep_columns:

            r.update({col:json_data[col]})

        results.append(r)
    
    return results

def merge_keys(dictionary, key_name):
    """
    Takes the higher level key of a dicionary and uses it
    as a value of a inner list of dicionaries. Useful
    to convert the dictionary into a pandas DataFrame.
    
    Args
    
        dictionary(dict): The dictionary which should have
                          a list of dictionaries in each
                          highest level value.
        key_name(str): The name to give to the highest level
                       keys once inside the new dictionaries.
    """
    
    results = []
    
    for key in dictionary.keys():
        for dict_ in dictionary[key]:
            dict_.update({key_name:key})
            results.append(dict_)
    
    return results

def _retrieve_query_settings(simple=False, add_method=False, ds=False):
    """
    Internal function to generate the necessary settings to do the query
    calls. 

    Args:

        simple(bool): Whether to call the simple API. If false it will
                      call the geosearch API.
        add_method(bool): Whether to add the method to call the API
                          in the settings. It is usually not necesssary
                          so it is set to False.
        ds(string): Whether to use the DataScience API instead of the 
                    geo. If evaluates to True must be a string that
                    contains the specific endpoint.

    Returns:

        dict: a dictionary containing the necessary parameters to call
              the API. By default the url, the path and the port.
    """
    
    url = 'http://ec2-13-229-144-6.ap-southeast-1.compute.amazonaws.com'
    
    if simple and not ds:
         path = 'ui_data_simple_query'
    elif not simple and not ds:
        path = 'ui_geosearch_query'
    elif ds:
        path = 'datascience/'+ds
   
    port = '5000'
    
    settings = {
    'url':url,
    'path':path,
    'port':port,
    }
    
    if add_method:
    
        settings['method'] = 'POST'

    return settings



def _match_key_to_actual(url, port, path, sk):
    """
    Internal function to match the search_key that the geolocalization API 
    returns to the corresponding actual value from the actual database.

    Args:

        url(str): The url of the APIs.
        port(str): The API's access port.
        path(str): The path from the API to call.
        sk(str): The internal search_key to search for the actuals
                 in the database. It is generated by the geolocalization
                 API.

    Returns:

        (requests.models.Response): a Response object with the extracted
                                    data.
    """
    
    data = {
    'search_key': sk,
    'sudo':False, # this means only actuals are used.
    }
    
    request = requests.post(f'{url}:{port}/{path}', json=data)

    return request.json()

def _parse_search_key_info(search_key):
    """
    Internal function to parse the search_key results so it extracts
    the corresponding metadata from the indicator.

    Args:

        search_key(str): The internal search_key to find the corresponding
                         actuals from a geolocalization query.

    Returns:

        (dict): A dict containing the indicator's metadata.
    """
    info = search_key.replace('ann %', 'ann%')
    info = info.split(' ')
    parsed_data = {
        'source_name': info[-4],
        'indicator_name' : info[-3],
        'uom': info[-2],
    }
    
    return parsed_data


def _search_by_distance(url, port, path, location, distance, distance_type, 
                        include_indicators_with_assets, folder_name,
                        indicator=False):
    """

    Internal function that calls the API for the geolocalization search.

    Args:

        url(str): The url of the APIs.
        port(str): The API's access port.
        path(str): The path from the API to call.
        sk(str): The internal search_key to search for the actuals
        location(str): The source point location. It is send to a geolocalization API
                       so it can take many forms like a city, and address.
        distance(int): The distance to draw the searching circle from the location.
        distance_type(str): Either 'km' for kilometers or 'miles'.
        include_indicators_with_assets(bool): If True only retrieves indicators with 
        an associated asset. If False only retrieve the ones without.
        folder_name(str): One of the available folder_names to look onto. Can be either:
                          'Any', 'Retail', 'Residential', 'Country Specific' or 'Office'.
    
    Returns:

        (dict): A dictionary with the requested data.
    """
    
    results = {}
    
    data = {
        'location_name': location,
        'distance': distance,
        'distance_type': distance_type.lower(),
        'folder_name':folder_name,
        'include_indicators_with_assets': include_indicators_with_assets
    }
    
    if indicator:
        data.update({'indicator_name_id':indicator})

    request = requests.post(f'{url}:{port}/{path}', json=data)

    try:    
        output = request.json()
    except:
        print('Request failed, see message error: ')
        print(request.content)
        return output

    for result in request.json()['responses'][folder_name]:
                
        sk = result['search_key']
        sk_parsed = _parse_search_key_info(sk)
        settings = _retrieve_query_settings(simple=True, add_method=False)
        result2 = _match_key_to_actual(**settings, sk=sk)
        results[sk] = sk_parsed
        results[sk]['responses'] = result2['responses'] 
    
    return results
