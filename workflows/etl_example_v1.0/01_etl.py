import aircraftlib as aclib

dulles_airport_position = aclib.Position(lat=38.9519444444, long=-77.4480555556)
area_surrounding_dulles = aclib.bounding_box(dulles_airport_position, radius_km=200)

# Extract: fetch data from multiple data sources
ref_data = aclib.fetch_reference_data()
raw_aircraft_data = aclib.fetch_live_aircraft_data(area=area_surrounding_dulles)

# Transform: clean the fetched data and add derivative data to aid in the analysis
live_aircraft_data = []
for raw_vector in raw_aircraft_data:
    vector = aclib.clean_vector(raw_vector)
    if vector:
        aclib.add_airline_info(vector, ref_data.airlines)
        live_aircraft_data.append(vector)

# Load: save the data for future analysis
db = aclib.Database()
db.add_live_aircraft_data(live_aircraft_data)
db.update_reference_data(ref_data)