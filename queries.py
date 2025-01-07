from sqlalchemy import create_engine, text
import pandas as pd

# Parametry połączenia z bazą danych
username = 'root'
password = 'Qwerty1234'
host = '127.0.0.1'
database = 'test'
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')


def fetch_stop_names():
    query = text("""
    SELECT 
        stop_id, 
        CONCAT(stop_name, ' ', @counter := IF(@stop_name = stop_name, @counter + 1, 1)) AS stop_name,
        stop_lat, stop_lon,
        @stop_name := stop_name
    FROM 
        (SELECT * FROM stops ORDER BY stop_name) AS order_stops, 
        (SELECT @counter := 0, @stop_name := '') AS vars
    """)
    with engine.connect() as conn:
        conn.execute(text("SET @counter = 0"))
        conn.execute(text("SET @stop_name = NULL"))
        df = pd.read_sql(query, con=conn)
    return df


def fetch_departure_times(stop_id):
    query = text("""
    SELECT 
        DATE_FORMAT(departure_time_planned, '%H:%i:%s') AS 'Planowy czas',
        DATE_FORMAT(departure_time_irl, '%H:%i:%s') AS 'Prognozowany czas',
        CAST(delay AS UNSIGNED) AS 'Opóźnienie [s]', 
        route_long_name AS 'Nr lini', 
        trip_headsign AS Kierunek
    FROM 
        trip_updates 
    NATURAL JOIN 
        trips 
    NATURAL JOIN 
        routes 
    WHERE 
        trip_updates.stop_id = :stop_id 
    ORDER BY 
        departure_time_irl
    """)
    df = pd.read_sql(query, con=engine, params={'stop_id': stop_id})
    return df


def fetch_vehicle_info(stop_id):
    query = text("""
    SELECT
        route_long_name,
        trip_headsign,
        license_plate, 
        latitude, 
        longitude, 
        stop_name, 
        CAST(distance AS UNSIGNED) AS distance 
    FROM 
        trip_updates 
    NATURAL JOIN 
        vehicle_positions 
    NATURAL JOIN 
        trips 
    NATURAL JOIN 
        routes 
    WHERE 
        trip_updates.stop_id = :stop_id
    """)
    df = pd.read_sql(query, con=engine, params={'stop_id': stop_id})
    return df


def fetch_route_names():
    query = "SELECT route_long_name FROM routes ORDER BY route_long_name"
    df = pd.read_sql(query, con=engine)
    return df


def fetch_destination_stops(route_long_name):
    query = text("""
        SELECT DISTINCT trip_headsign 
        FROM routes NATURAL JOIN trips 
        WHERE routes.route_long_name = :route_long_name
    """)
    df = pd.read_sql(query, con=engine, params={'route_long_name': route_long_name})
    return df


def fetch_trips_by_headsign_and_time(trip_headsign, date, hour):
    query = text("""
        SELECT DISTINCT trip_id 
        FROM trips NATURAL JOIN delays 
        WHERE trip_headsign = :trip_headsign 
          AND DATE(timestamp) = :date 
          AND HOUR(timestamp) = :hour
    """)
    df = pd.read_sql(query, con=engine, params={
        'trip_headsign': trip_headsign,
        'date': date,
        'hour': hour
    })
    return df


def fetch_delays(trip_id, date, hour):
    query = text("""
        SELECT 
            d1.trip_id, 
            s.stop_name, 
            CAST(d1.delay AS UNSIGNED) AS delay,
            d1.timestamp
        FROM 
            delays d1
        JOIN (
            SELECT 
                stop_id, 
                MAX(timestamp) AS max_timestamp
            FROM 
                delays
            WHERE 
                trip_id = :trip_id 
                AND DATE(timestamp) = :date 
                AND HOUR(timestamp) >= :hour
            GROUP BY 
                stop_id
        ) d2 ON d1.stop_id = d2.stop_id AND d1.timestamp = d2.max_timestamp
        JOIN 
            stops s ON d1.stop_id = s.stop_id
        WHERE 
            d1.trip_id = :trip_id;
    """)
    df = pd.read_sql(query, con=engine, params={
        'trip_id': trip_id,
        'date': date,
        'hour': hour
    })
    return df


def positions():
    # pobieramy przystanki z bazy aby uzytkownik wybral jaki go interesuje
    stop_data = fetch_stop_names()
    print('stop_data.iloc[0]', stop_data.iloc[0], sep='\n')
    print()

    # znajac id przystanku pobieramy z bazy informacje o aktualnych kursach z danego przystanku
    departure_data = fetch_departure_times(stop_data['stop_id'].iloc[0])
    print('departure_data.head()', departure_data.head(), sep='\n')
    print()

    # znajac id przystanku pobieramy z bazy informacje o pojazdach jadących do danego przystanku
    vehicle_data = fetch_vehicle_info(stop_data['stop_id'].iloc[0])
    print('vehicle_data.head()', vehicle_data.head(), sep='\n')

def delays():
    # pobieramy numery linii aby uzytkownik wybral jaki go interesuje
    route_data = fetch_route_names()
    print('route_data.iloc[0]', route_data.iloc[0], sep='\n')
    print()

    # pobieramy kierunki lini aby uzytkownik wybral jaki go interesuje
    destination_data = fetch_destination_stops(route_data['route_long_name'].iloc[0])
    print('destination_data.head()', destination_data.head(), sep='\n')
    print()

    # pobieramy kursy jakie się odbywają/odbywały w danym kierunku konkretnego dnia od podanej godziny włącznie aby uzytkownik wybral jaki go interesuje
    trips_data = fetch_trips_by_headsign_and_time(destination_data['trip_headsign'].iloc[0], '2025-01-06', 19)
    print('trips_data.head()', trips_data.head(), sep='\n')
    print()

    # mając juz wybrany kurs pobieramy dane dotyczace opoznien
    delays_data = fetch_delays(trips_data['trip_id'].iloc[0], '2025-01-06', 19)
    print('delays_data.head()', delays_data.head(), sep='\n')
    print()

if __name__ == '__main__':
    positions()
    # delays()
