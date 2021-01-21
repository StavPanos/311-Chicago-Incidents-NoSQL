import pandas as pd

type_list = ["abandoned-vehicles",
"alley-lights-out",
"garbage-carts",
"graffiti-removal",
"pot-holes-reported",
"rodent-baiting",
"sanitation-code-complaints",
"street-lights-all-out",
"street-lights-one-out",
"tree-debris",
"tree-trims",
]

incident_type = "graffiti-removal"
# service request type codification
service_request_type = "4"

# Read the file
chunk = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/Projects/311-Incidents/csv/311-service-requests-" + incident_type + ".csv",
                   low_memory=False, chunksize=100000)

data = pd.concat(chunk)

# Rename columns
data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'type_of_service_request': 'service_request_type_descr.string()'}, inplace=True)
data.rename(columns={'zip': 'zip_code.int32()'}, inplace=True)
data.rename(columns={'zip_code': 'zip_code.int32()'}, inplace=True)
data.rename(columns={'historical_wards_2003-2015': 'historical_wards.string()'}, inplace=True)

data.rename(columns={'status': 'status.string()'}, inplace=True)
data.rename(columns={'service_request_number': 'service_request_number.string()'}, inplace=True)
data.rename(columns={'street_address': 'street_address.string()'}, inplace=True)
data.rename(columns={'x_coordinate': 'x_coordinate.double()'}, inplace=True)
data.rename(columns={'y_coordinate': 'y_coordinate.double()'}, inplace=True)
data.rename(columns={'ward': 'ward.int32()'}, inplace=True)
data.rename(columns={'police_district': 'police_district.int32()'}, inplace=True)
data.rename(columns={'community_area': 'community_area.int32()'}, inplace=True)
data.rename(columns={'latitude': 'latitude.double()'}, inplace=True)
data.rename(columns={'longitude': 'longitude.double()'}, inplace=True)
data.rename(columns={'location': 'location.string()'}, inplace=True)

data.rename(columns={'zip_codes': 'zip_codes.int32()'}, inplace=True)
data.rename(columns={'community_areas': 'community_areas.int32()'}, inplace=True)
data.rename(columns={'census_tracts': 'census_tracts.int32()'}, inplace=True)
data.rename(columns={'wards': 'wards.int32()'}, inplace=True)

data.rename(columns={'current_activity': 'current_activity.string()'}, inplace=True)
data.rename(columns={'most_recent_action': 'most_recent_action.string()'}, inplace=True)
data.rename(columns={'ssa': 'ssa.int32()'}, inplace=True)

# SERVICE REQUEST TYPE CODE
data['service_request_type.int32()'] = service_request_type

data['total_upvotes.int32()'] = "0"


def format_dates():
    data['creation_date'] = data.creation_date.str.replace('T', ' ')

    data.completion_date = data.completion_date.fillna('')
    data['completion_date'] = data.completion_date.str.replace('T', ' ')

    data.rename(columns={'creation_date': 'creation_date.date_ms(M/d/yyyy)'}, inplace=True)
    data.rename(columns={'completion_date': 'completion_date.date_ms(M/d/yyyy)'}, inplace=True)

    data['creation_date.date_ms(M/d/yyyy)'] = data['creation_date.date_ms(M/d/yyyy)'].str[:10]
    data['completion_date.date_ms(M/d/yyyy)'] = data['completion_date.date_ms(M/d/yyyy)'].str[:10]


def clean_vehicle():
    data.rename(columns={'vehicle_make/model': 'make_model.string()'}, inplace=True)
    data.rename(columns={'vehicle_color': 'color.string()'}, inplace=True)
    data.rename(columns={'how_many_days_has_the_vehicle_been_reported_as_parked?': 'abandoded_days.string()'}, inplace=True)

    data.loc[data['abandoded_days.string()'] > 9223372036854775807, 'abandoded_days.string()'] = 9999999
    data.loc[data['abandoded_days.string()'] < 0, 'abandoded_days.string()'] = 0
    data['color.string()'] = data['color.string()'].str.replace(' ', '')
    data.loc[data['color.string()'].str.len() > 10, 'color.string()'] = 'Multi-color'
    data['license_plate'] = data['license_plate'].str.replace('  ', '')
    data['license_plate'] = data['license_plate'].str.strip().str.upper()

    symbols = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '=', '+', '?', ';', '<', '>', '.', ',', '\'',
               '\"', 'none',
               'unknown', 'unkown', 'not ', 'no ', 'missing', 'xxxxxx', '-----']

    for tmp in symbols:
        data.license_plate = data.license_plate.apply(lambda x: '' if tmp in str(x).lower() else x)

    data['license_plate'] = data['license_plate'].str[:50]

    data.rename(columns={'license_plate': 'license_plate.string()'}, inplace=True)


def clean_garbage():
    data.rename(columns={'number_of_black_carts_delivered': 'carts_delivered.double()'}, inplace=True)
    data.loc[data['carts_delivered.double()'] < 0, 'carts_delivered.double()'] = 0


def clean_graffiti():
    data.rename(columns={'what_type_of_surface_is_the_graffiti_on?': 'surface.string()'}, inplace=True)
    data.rename(columns={'where_is_the_graffiti_located?': 'located_at.string()'}, inplace=True)


def clean_pot_holes():
    data.rename(columns={'number_of_potholes_filled_on_block': 'filled_pot_holes.double()'}, inplace=True)


def clean_rodent_baiting():
    data.rename(columns={'number_of_premises_baited': 'premises_baited.double()'}, inplace=True)
    data.rename(columns={'number_of_premises_with_garbage': 'with_garbage.double()'}, inplace=True)
    data.rename(columns={'number_of_premises_with_rats': 'with_rats.double()'}, inplace=True)

    data.loc[data['premises_baited.double()'] < 0, 'premises_baited.double()'] = 0
    data.loc[data['with_garbage.double()'] < 0, 'with_garbage.double()'] = 0
    data.loc[data['with_rats.double()'] < 0, 'with_rats.double()'] = 0


def clean_sanitation():
    data.rename(columns={'what_is_the_nature_of_this_code_violation?': 'code_nature.string()'}, inplace=True)


def clean_tree():
    data.rename(columns={'if_yes,_where_is_the_debris_located?': 'located_at.string()'}, inplace=True)
    data.rename(columns={'location_of_trees': 'located_at.string()'}, inplace=True)

format_dates()
#clean_vehicle()
#clean_garbage()
clean_graffiti()
#clean_pot_holes()
#clean_rodent_baiting()
#clean_sanitation()
#clean_tree()

#df1 = data.iloc[:500000]
#df2 = data.iloc[500000:]

# Output the number of rows and column names
print("\nTotal rows: {0}".format(len(data)))

print(data.columns, "\n")

print(data.columns, "\n")

print(data.dtypes)

# Write to new csv
data.to_csv(
   "C:/Users/stavropoulosp/PycharmProjects/311-Chicago-Incidents-NoSQL/csv/final/311-service-requests-" + incident_type + ".csv",
   index=False, header=True)