from weakref import ref
import aircraftlib as aclib
from prefect import task, Flow


@task
def extract_reference_data():
    print('Fetching reference data...')
    return aclib.fetch_reference_data()

@task
def extract_live_data():
    
    dulles_airport_position = aclib.Position(lat=38.9519444444, long=-77.4480555556)
    area_surronding_dulles = aclib.bounding_box(dulles_airport_position, radius_km=200)

    print("fetching live aircraft data...")
    raw_aircraft_data = aclib.fetch_live_aircraft_data(area=area_surronding_dulles)

    return raw_aircraft_data

@task
def transform(raw_aircraft_data, ref_data):
    print('Cleaning & Transforming aircraft data...')

    live_aircraft_data =[]
    for raw_vector in raw_aircraft_data:
        vector = aclib.clean_vector(raw_vector)
        if vector:
            aclib.add_airline_info(vector, ref_data.airlines)
            live_aircraft_data.append(vector)

    return live_aircraft_data

@task
def load_reference_data(ref_data):
    print('Saving reference data...')
    db = aclib.Database()
    db.update_reference_data(ref_data)

@task
def load_live_data(live_aircraft_data):
    print('Saving live aircraft data...')
    db = aclib.Database()
    db.add_live_aircraft_data(live_aircraft_data)


@Flow(name='02 Aircraft ETL')
def etl_aircraft_flow():
    reference_data = extract_reference_data()
    live_data = extract_live_data()

    transformed_live_data = transform(live_data, reference_data)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)

if __name__ == "__main__":
    etl_aircraft_flow()