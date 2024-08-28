from robocorp.tasks import task
from robocorp import workitems
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables


#create objects 
http = HTTP()
json = JSON()
table = Tables()

# traffic json file path
traffic_json_file_path = "output/traffic_data.json"

# Json keys
COUNTRY_KEY = "SpatialDim"
GENDER_KEY = "Dim1"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"


@task
def produce_traffic_data():

    download_traffic_data()
    traffic_data = load_traffic_data_as_table()
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    filtered_data = get_latest_data_by_country(filtered_data)
    payloads = create_work_item_payloads(filtered_data)
    save_work_item_payloads(payloads)
    # table.write_table_to_csv(filtered_data, "output/filtered_traffic_data.csv")
    

def download_traffic_data():
    http.download(
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file=traffic_json_file_path,
        overwrite=True
    )

# load jason and convert to table
def load_traffic_data_as_table():
    json_data = json.load_json_from_file(traffic_json_file_path)
    return table.create_table(json_data["value"])


def filter_and_sort_traffic_data(data): 
    Max_rate = 5.0
    both_genders = "BTSX"
    
    table.filter_table_by_column(data, RATE_KEY, "<", Max_rate)
    table.filter_table_by_column(data, GENDER_KEY, "==", both_genders)
    table.sort_table_by_column(data, RATE_KEY, False)
    return data
   

def get_latest_data_by_country(data):

    data = table.group_table_by_column(data, COUNTRY_KEY)
    latest_data_by_country = []

    for group in data:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row)

    return latest_data_by_country


def create_work_item_payloads(filtered_data):
    payloads = []
    for row in filtered_data:
        payload = dict(
            country = row[COUNTRY_KEY],
            year = row[YEAR_KEY],
            rate = row[RATE_KEY]
        )
        payloads.append(payload)
    
    return payloads


def save_work_item_payloads(payloads):
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)
