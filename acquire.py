import requests
import pandas as pd

# Function to acquire SWAPI data for a category and save it to a CSV file
def acquire_swapi_data(category, filename):
    # Define the SWAPI base URL
    swapi_base_url = 'https://swapi.dev/api/'

    # Initialize an empty list to store data from all pages
    all_results = []

    # Initialize the URL for the first page
    next_url = f'{swapi_base_url}{category}/'

    # Continue making requests until there are no more pages
    while next_url:
        # Make an HTTP GET request to the current page
        response = requests.get(next_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON content of the response
            data = response.json()

            # Append the 'results' from the current page to the list
            all_results.extend(data['results'])

            # Get the URL of the next page, or None if no more pages
            next_url = data['next']
        else:
            print(f'Failed to acquire data for {category}')
            return None

    # Create a DataFrame from the combined results
    df = pd.DataFrame(all_results)

    # Save the DataFrame to a CSV file
    df.to_csv(filename, index=False)
    return df



def acquire_opsd_data(url):
    """
    Acquire the Open Power Systems Data (OPSD) for Germany from the given URL.

    Args:
    url (str): The URL of the OPSD data in CSV format.

    Returns:
    pd.DataFrame: A DataFrame containing the OPSD data.
    """
    try:
        # Read the OPSD data into a DataFrame
        filename = ('opsd_germany_daily.csv')
        opsd_df = pd.read_csv(url)
        opsd_df.to_csv(filename, index = False)
        return opsd_df
    except Exception as e:
        print(f"Error: {e}")
        return None
