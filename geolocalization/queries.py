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
                       include_indicators_with_assets=True, folder_name='Any'):
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
                                  include_indicators_with_assets=include_indicators_with_assets)
    return results

def return_all_national_indicators(location='Sydney'):
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
        'only_nationals':True,
    }
    
    request = requests.post(f'{url}:{port}/{path}', json=data)
    
    results = {}

    json_out = request.json()

    for tag in json_out['responses']:
        for result in request.json()['responses'][tag]:
            sk = result['search_key']
            sk_parsed = _parse_search_key_info(sk)
            settings = _retrieve_query_settings(simple=True, add_method=False)
            result2 = _match_key_to_actual(**settings, sk=sk)
            results[sk] = sk_parsed
            results[sk]['responses'] = result2.json()['responses']
	    
    return results


def _retrieve_query_settings(simple=False, add_method=False):
    """
    Internal function to generate the necessary settings to do the query
    calls. 

    Args:

        simple(bool): Whether to call the simple API. If false it will
                      call the geosearch API.
        add_method(bool): Whether to add the method to call the API
                          in the settings. It is usually not necesssary
                          so it is set to False.

    Returns:

        dict: a dictionary containing the necessary parameters to call
              the API. By default the url, the path and the port.
    """
    
    url = 'http://ec2-13-229-144-6.ap-southeast-1.compute.amazonaws.com'
    
    if simple:
         path = 'ui_data_simple_query'
    else:
        path = 'ui_geosearch_query'
   
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
    'sudo':False, # para cojer solo actuals
    }
    
    request = requests.post(f'{url}:{port}/{path}', json=data)
    
    return request

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
                        include_indicators_with_assets, folder_name):
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
    
    request = requests.post(f'{url}:{port}/{path}', json=data)
    

    for result in request.json()['responses'][folder_name]:
                
        sk = result['search_key']
        sk_parsed = _parse_search_key_info(sk)
        settings = _retrieve_query_settings(simple=True, add_method=False)
        result2 = _match_key_to_actual(**settings, sk=sk)
        results[sk] = sk_parsed
        results[sk]['responses'] = result2.json()['responses'] 
    
    return results
