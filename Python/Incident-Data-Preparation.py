import pandas as pd

incident_type = "abandoned-vehicles"
# service request type codification
service_request_type = "1"

# Read the file
data = pd.read_csv(
    "C:/Users/stavropoulosp/Desktop/M149/Projects/311-Incidents/csv/311-service-requests-" + incident_type + ".csv",
    low_memory=False)

# Rename columns
data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'type_of_service_request': 'service_request_type_descr'}, inplace=True)
data.rename(columns={'zip': 'zip_code'}, inplace=True)
data.rename(columns={'historical_wards_2003-2015': 'historical_wards'}, inplace=True)

# SERVICE REQUEST TYPE CODE
data['service_request_type'] = service_request_type

data['total_upvotes'] = "0"


def format_dates():
    data['creation_date'] = data.creation_date.str.replace('T', ' ')

    data.completion_date = data.completion_date.fillna('')
    data['completion_date'] = data.completion_date.str.replace('T', ' ')

    data['creation_date'] = data['creation_date'].str[:19]
    data['completion_date'] = data['completion_date'].str[:19]


def clean_vehicle():
    data.rename(columns={'vehicle_make/model': 'make_model'}, inplace=True)
    data.rename(columns={'vehicle_color': 'color'}, inplace=True)
    data.rename(columns={'how_many_days_has_the_vehicle_been_reported_as_parked?': 'abandoded_days'}, inplace=True)

    data.loc[data['abandoded_days'] > 9223372036854775807, 'abandoded_days'] = 9999999
    data.loc[data['abandoded_days'] < 0, 'abandoded_days'] = 0
    data['color'] = data['color'].str.replace(' ', '')
    data.loc[data['color'].str.len() > 10, 'color'] = 'Multi-color'
    data['license_plate'] = data['license_plate'].str.replace('  ', '')
    data['license_plate'] = data['license_plate'].str.strip().str.upper()

    symbols = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '=', '+', '?', ';', '<', '>', '.', ',', '\'',
               '\"', 'none',
               'unknown', 'unkown', 'not ', 'no ', 'missing', 'xxxxxx', '-----']

    for tmp in symbols:
        data.license_plate = data.license_plate.apply(lambda x: '' if tmp in str(x).lower() else x)

    data['license_plate'] = data['license_plate'].str[:50]


# Output the number of rows and column names
print("\nTotal rows: {0}".format(len(data)))
print(data.columns, "\n")

print(data.dtypes)

format_dates()
clean_vehicle()

# Write to new csv
data.to_csv(
    "C:/Users/stavropoulosp/PycharmProjects/311-Chicago-Incidents-NoSQL/csv/" + service_request_type + "/311-service-requests-" + incident_type + ".csv",
    index=False, header=True)
