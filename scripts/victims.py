"""

Author: Alessandro Chávez
Description:
    Script that consumes victims related data to do the following:
        1. 

"""

from ast import main
import pandas as pd
import numpy as np


def load_data(prefix='./data/') -> list:
    """TBD

    :param prefix: _description_, defaults to './data/'
    :type prefix: str, optional
    :return: _description_
    :rtype: list
    """

    # Define list where to store all input dataframes
    all_input_dfs = []

    # Define metadata of files to ingest
    # TODO: Move this to a function that loads the metadata from a .YAML file
    files_metadata = [
        {
            'filename': 'accidental_deaths_2021_to_date.csv',
            'df_name': 'Accidental Deaths 2021 To Date'
        },
        {
            'filename': 'accidental_injuries_2021_to_date.csv',
            'df_name': 'Accidental Injuries 2021 To Date'
        },
        {
            'filename': 'children_killed_2021_to_date.csv',
            'df_name': 'Children Killed 2021 To Date'
        },
        {
            'filename': 'children_injured_2021_to_date.csv',
            'df_name': 'Children Injured 2021 To Date'
        },
        {
            'filename': 'teens_killed_2021_to_date.csv',
            'df_name': 'Teens Killed 2021 To Date'
        },
        {
            'filename': 'teens_injured_2021_to_date.csv',
            'df_name': 'Teens Injured 2021 To Date'
        },
        {
            'filename': 'mass_shootings_Injured_killed_2021_to_date.csv',
            'df_name': 'Mass Shootings Injured Killed 2021 To Date'
        }
    ]

    # Loop thru the metadata and ingest each of the desired input files
    # TODO: Implement exception handling for invalid files
    for input_file in files_metadata:
        filename = f"{prefix}{input_file['filename']}"
        df_name = input_file['df_name']
        tmp_df = pd.read_csv(filename, header=0)
        tmp_df.name = df_name
        all_input_dfs.append(tmp_df)

    return all_input_dfs


def enrich_data(all_input_dfs):
    """TBD

    :param all_input_dfs: _description_
    :type all_input_dfs: _type_
    """

    for df in all_input_dfs:

        # Drop "Operations" column since it doesn't offer any value
        df.drop('Operations', axis=1, inplace=True)

        # Add new "Children Involved" column to specify if the data has to do with
        # children
        if 'children' in df.name.lower():
            df.insert(7, 'Children Involved', True)
        else:
            df.insert(7, 'Children Involved', np.nan)

        # Add new "Teens Involved" column to specify if the data has to do with
        # children
        if 'teens' in df.name.lower():
            df.insert(8, 'Teens Involved', True)
        else:
            df.insert(8, 'Teens Involved', np.nan)

        # Add new "Accident" column to specify if the data has to do with accidents
        if 'accident' in df.name.lower():
            df.insert(9, 'Accident', True)
        else:
            df.insert(9, 'Accident', np.nan)

        # Add new "Mass Shooting" column to specify if the data has to do with
        # mass shootings
        if 'mass shooting' in df.name.lower():
            df.insert(10, 'Mass Shooting', True)
        else:
            df.insert(10, 'Mass Shooting', np.nan)


def generate_final_dataset(all_input_records):
    """TBD

    :param all_input_records: _description_
    :type all_input_records: _type_
    """

    all_victims_df = pd.DataFrame(
        {
            'Incident ID': pd.Series(dtype='int16'),
            'Incident Date': pd.Series(dtype='datetime64[ns]'),
            'State': pd.Series(dtype='object'),
            'City Or County': pd.Series(dtype='object'),
            'Address': pd.Series(dtype='object'),
            '# Killed': pd.Series(dtype='int8'),
            '# Injured': pd.Series(dtype='int8'),
            'Children Involved': pd.Series(dtype='bool'),
            'Teens Involved': pd.Series(dtype='bool'),
            'Accident': pd.Series(dtype='bool'),
            'Mass Shooting': pd.Series(dtype='bool')
        },
        index=['Incident ID']
    )

    # Concatenate the records from all the dataframes into a single final dataframe
    for df in all_input_records:
        df.set_index('Incident ID', inplace=True, drop=False)
        all_victims_df.update(df, overwrite=False)
        all_victims_df = pd.concat(
            [all_victims_df, df[~df.index.isin(all_victims_df.index)]])

    # Remove duplicated header row
    all_victims_df.dropna(inplace=True, how='all')

    # Replace NAN values with False (the NAN values were intentional for making the
    # update process easier)
    all_victims_df.replace(np.nan, False, inplace=True)

    # Make the Incident ID column an integer again
    all_victims_df = all_victims_df.astype({'Incident ID': int})

    # Sort the records by the Incident Date (ASC)
    all_victims_df.sort_values(by='Incident Date', inplace=True)

    # Break datetime column into multiple columns for the date dimension table
    all_victims_df['Year'] = pd.DatetimeIndex(
        all_victims_df['Incident Date']).year
    all_victims_df['Quarter'] = pd.DatetimeIndex(
        all_victims_df['Incident Date']).quarter
    all_victims_df['Month'] = pd.DatetimeIndex(
        all_victims_df['Incident Date']).month
    all_victims_df['Week'] = pd.DatetimeIndex(
        all_victims_df['Incident Date']).week
    all_victims_df['Weekday'] = pd.DatetimeIndex(
        all_victims_df['Incident Date']).weekday

    return all_victims_df


def main():
    """Ingest input files with data related to victims in the USA. This function
    will execute the following steps:
    1. Load data from different CSVs in the form of Pandas Dataframes
    2. Create a new dataframe which will contain the data from all the input
       CSVs
    3. Transform input dataframes to include columns with useful data for the
       datawarehouse tables
        3.1 Add columns to flag the records that have data related to children,
            teens, accidents, mass shootings
    4. Concatenate all input dataframes into a single final dataframe
        4.1 Execute upsert type of logic
    5. Add columns required to build date dimension table
    """

    # Load input CSV data in the form of dataframes
    all_input_dfs = load_data()

    enrich_data(all_input_dfs)

    all_victims_df = generate_final_dataset(all_input_dfs)

    # TODO: Move this to a function with configurable output folder...
    all_victims_df.to_csv('./output/all_victims_2021_to_date.csv')


if __name__ == '__main__':
    main()
