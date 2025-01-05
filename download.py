import os
import requests
from google.transit import gtfs_realtime_pb2
import time
import pandas as pd
from datetime import datetime
import threading
import timeit
import zipfile

URL = 'https://gtfs.ztp.krakow.pl/'
trip_updates_filename = 'TripUpdates_T.pb'
vehicle_positions_filename = 'VehiclePositions_T.pb'
schedule_filename = 'GTFS_KRK_T.zip'
schedule_path = './downloads'


# pobiera i przetwarza pliki pb
def get_pb_file(url: str, name: str, result: dict, key: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get(URL + name)
        response.raise_for_status()
        feed.ParseFromString(response.content)
        result[key] = feed
    except requests.RequestException as e:
        print(f"Błąd podczas pobierania danych: {e}")
        result[key] = None
    except google.protobuf.message.DecodeError as e:
        print(f"Błąd podczas parsowania wiadomości: {e}")
        result[key] = None


# pobiera archiwum z rozkładem
def get_schedule(url: str, name: str, path: str):
    try:
        response = requests.get(URL + name)
        response.raise_for_status()
        zip_path = os.path.join(path, name)
        with open(zip_path, 'wb') as f:
            f.write(response.content)
    except requests.RequestException as e:
        print(f"Błąd podczas pobierania danych: {e}")


# przetwarza dane trip updates na postać df
def trip_update_to_df(feed: gtfs_realtime_pb2.FeedMessage, result: dict):
    trip_updates = []
    for entity in feed.entity:
        trip_update = entity.trip_update

        if trip_update:
            for stop_time_update in trip_update.stop_time_update:
                trip_updates.append({
                    "trip_id": trip_update.trip.trip_id,
                    "stop_id": stop_time_update.stop_id,
                    "arrival_time": datetime.fromtimestamp(
                        stop_time_update.arrival.time) if stop_time_update.HasField("arrival") else None,
                    "departure_time": datetime.fromtimestamp(
                        stop_time_update.departure.time) if stop_time_update.HasField(
                        "departure") else None
                })
    result['trip_updates'] = pd.DataFrame(trip_updates)


# przetwarza dane vehicle positions na postać df
def vehicle_positions_to_df(feed: gtfs_realtime_pb2.FeedMessage, result: dict):
    vehicle_positions = []
    for entity in feed.entity:
        vehicle_positions.append({
            'trip_id': entity.vehicle.trip.trip_id,
            'license_plate': entity.vehicle.vehicle.license_plate,
            'latitude': entity.vehicle.position.latitude,
            'longitude': entity.vehicle.position.longitude,
            'stop_id': entity.vehicle.stop_id,
            'timestamp': datetime.fromtimestamp(entity.vehicle.timestamp)
        })
    result['vehicle_positions'] = pd.DataFrame(vehicle_positions)


# przetwarza rozkład na df
def schedule_to_df(name: str, path: str, result: dict):
    zip_path = os.path.join(path, name)
    if not os.path.exists(zip_path):
        print(f"Plik {zip_name} nie istnieje.")
        result[key] = None

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)

    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if file == 'trips.txt':
            result['trips'] = pd.read_csv(file_path, sep=',')[['trip_id', 'route_id', 'trip_headsign']]
        elif file == 'stop_times.txt':
            result['stop_times'] = pd.read_csv(file_path, sep=',')[
                ['trip_id', 'arrival_time', 'departure_time', 'stop_id']]
            result['stop_times']['arrival_time'] = pd.to_datetime(
                result['stop_times']['arrival_time'],
                format='%H:%M:%S',
                errors='coerce').apply(
                lambda x: x.replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
            )
        elif file == 'routes.txt':
            result['routes'] = pd.read_csv(file_path, sep=',')[['route_id', 'route_long_name']]
        elif file == 'stops.txt':
            result['stops'] = pd.read_csv(file_path, sep=',')[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
        os.remove(file_path)


# zwraca dwa dataframe z danymi o trasach i pojazdach, oraz próbkę czasu
def download_realtime_info() -> dict:
    download_results = {}
    get_threads = [
        threading.Thread(target=get_pb_file, args=(URL, trip_updates_filename, download_results, 'trip_updates')),
        threading.Thread(target=get_pb_file,
                         args=(URL, vehicle_positions_filename, download_results, 'vehicle_positions')),
        threading.Thread(target=get_schedule, args=(URL, schedule_filename, schedule_path)),
    ]

    for thread in get_threads:
        thread.start()

    for thread in get_threads:
        thread.join()

    if (not download_results['trip_updates'] or not download_results['trip_updates'] or
            download_results['trip_updates'].header.timestamp != download_results[
                'vehicle_positions'].header.timestamp):
        time.sleep(1)
        return download_realtime_info()

    else:
        processing_results = {}
        proccesing_threads = [
            threading.Thread(target=trip_update_to_df,
                             args=(download_results['trip_updates'], processing_results)),
            threading.Thread(target=vehicle_positions_to_df,
                             args=(download_results['vehicle_positions'], processing_results)),
            threading.Thread(target=schedule_to_df,
                             args=(schedule_filename, schedule_path, processing_results))
        ]

        for thread in proccesing_threads:
            thread.start()
        for thread in proccesing_threads:
            thread.join()

        processing_results['timestamp'] = pd.DataFrame({'timestamp': [datetime.fromtimestamp(
            download_results['trip_updates'].header.timestamp).strftime('%Y-%m-%d %H:%M:%S')]})
    return processing_results


if __name__ == '__main__':
    # czas_wykonania = timeit.timeit(download_realtime_info, number=1)
    # print(f"Czas wykonania: {czas_wykonania} sekund")
    data = download_realtime_info()
    print(data.keys())

