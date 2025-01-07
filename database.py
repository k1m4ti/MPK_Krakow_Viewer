from download import download_realtime_info
import threading
import time
import pandas as pd
from geopy.distance import geodesic
from sqlalchemy import create_engine
import timeit

# parametry połączenia do bazy
username = 'root'
password = 'Qwerty1234'
host = '127.0.0.1'
database = 'test'
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')

# funkcja do liczenia odległości za pomocą współrzędnych
def calculate_distance(lat1, lon1, lat2, lon2):
    point1 = (lat1, lon1)
    point2 = (lat2, lon2)
    return geodesic(point1, point2).meters

lock = threading.Lock()
data = None

# pobiera nowe dane z serwera po przetworzeniu poprzedniej partii
def update_data():
    global data
    with lock:
        data = download_realtime_info()
        print('Aktualizacja danych przebiegła pomyślnie')

# transformuje dane w celu wyciągania informacji
def transform_data():
    global data
    with lock:
        # filtracja tylko tras w "trakcie przejazdu"
        data['trip_updates'] = data['trip_updates'][data['trip_updates']['trip_id'].isin(data['vehicle_positions']['trip_id'])]
        data['trip_updates'] = data['trip_updates'].sort_values(by=['trip_id', 'departure_time'])

        
        # połączenie planowanego przasu przyjazdu z rzeczywistym
        data['trip_updates'] = pd.merge(data['stop_times'], data['trip_updates'], on=['trip_id', 'stop_id'], suffixes=('_planned', '_irl'))
        data['trip_updates']['delay'] = (data['trip_updates']['departure_time_irl'] - data['trip_updates']['departure_time_planned']).dt.total_seconds()

        # obliczamy odległości pojazdów od przystanku
        data['vehicle_positions'] = pd.merge(data['vehicle_positions'], data['stops'], on='stop_id')
        data['vehicle_positions']['distance'] = data['vehicle_positions'].apply(
            lambda row: calculate_distance(row['latitude'], row['longitude'], row['stop_lat'], row['stop_lon']),
            axis=1
        )

        print('Dane zostały przetransformowane')


# zapisuje przetransformowane dane do bazy danych
def data_to_sql():
    global data
    with lock:
        data['trip_updates'][['trip_id', 'stop_id', 'departure_time_planned', 'departure_time_irl', 'delay']].to_sql('trip_updates', con=engine, if_exists='replace', index=False)
        # zmienić na dopisywanie
        data['trip_updates'][['trip_id', 'stop_id', 'delay', 'timestamp']].to_sql('delays', con=engine, if_exists='append', index=False)
        data['vehicle_positions'][['trip_id', 'license_plate', 'latitude', 'longitude', 'stop_name', 'distance']].to_sql('vehicle_positions', con=engine, if_exists='replace', index=False)
        data['stops'].to_sql('stops', con=engine, if_exists='replace', index=False)
        data['routes'].to_sql('routes', con=engine, if_exists='replace', index=False)
        data['trips'].to_sql('trips', con=engine, if_exists='replace', index=False)

        print('Dane zostały zapisane do bazy danych')

# od pobrania danych do zapisania ich w bazie
def load_transform_store():
    threads = [
        threading.Thread(target=update_data),
        threading.Thread(target=transform_data),
        threading.Thread(target=data_to_sql)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    # czas_wykonania = timeit.timeit(load_transform_store, number=1)
    # print(f"Czas wykonania: {czas_wykonania} sekund")
    # load_transform_store()
    i = 0
    while True:
        load_transform_store()
        time.sleep(6)
        print(i)
        i += 1
