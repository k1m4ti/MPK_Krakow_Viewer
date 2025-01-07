import tkinter as tk
from tkinter import ttk, messagebox
from queries import *
import pandas as pd
import tkintermapview
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from datetime import datetime


class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MPK krakow")
        self.geometry("1200x800")

        self.notebook = ttk.Notebook(self)
        self.tab1 = ttk.Frame(self.notebook)
        self.tab2 = ttk.Frame(self.notebook)

        self.notebook.add(self.tab1, text="Aktualne odjazdy")
        self.notebook.add(self.tab2, text="Wykresy opóźnień")
        self.notebook.pack(expand=1, fill=tk.BOTH)

        self.setup_tab1()
        self.setup_tab2()

        self.refresh_interval = 10000

    # zakładka z mapą
    def setup_tab1(self):
        # menu wyboru przystanku
        frame_top = ttk.Frame(self.tab1)
        frame_top.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        self.stop_var = tk.StringVar()
        self.stop_data = fetch_stop_names()

        ttk.Label(frame_top, text="Przystanek:").pack(side=tk.LEFT, padx=5, pady=5)
        self.stop_menu = ttk.Combobox(frame_top, textvariable=self.stop_var, state="readonly")
        self.stop_menu['values'] = self.stop_data['stop_name'].tolist()
        self.stop_menu.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(frame_top, text="Zatwierdź", command=self.fetch_stop_data).pack(side=tk.LEFT, padx=5, pady=5)

        
        # ramka dla pozostałych elementów
        main_frame = ttk.Frame(self.tab1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # podramki dla tableli i mapy
        self.departure_frame = ttk.LabelFrame(main_frame, text="Tabela odjazdów")
        self.departure_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        self.map_frame = ttk.LabelFrame(main_frame, text="Podgląd na mapie (częstotliwość odświeżania 10s)")
        self.map_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        
        # konfiguracja tabeli
        self.show_table(self.departure_frame, pd.DataFrame(columns=['Planowy czas', 'Prognozowany czas', 'Opóźnienie [s]', 'Nr lini', 'Kierunek']))

        # Konfiguracja proporcji
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=3)

        # konfiguracja mapy
        self.map_view = tkintermapview.TkinterMapView(self.map_frame, width=800, height=600, corner_radius=0)
        self.map_view.pack(fill=tk.BOTH, expand=True)
        self.map_view.set_tile_server("https://mt0.google.com/vt/lyrs=m&hl=en&x={x}&y={y}&z={z}&s=Ga", max_zoom=22)
        self.map_view.set_position(50.0647, 19.9450)

    # pobieranie danych z bazy
    def fetch_stop_data(self):
        selected_stop = self.stop_var.get()
        if not selected_stop:
            return
        
        stop_id = self.stop_data.loc[self.stop_data['stop_name'] == selected_stop]['stop_id'].iloc[0]

        departure_data = fetch_departure_times(stop_id)
        vehicle_data = fetch_vehicle_info(stop_id)

        self.show_table(self.departure_frame, departure_data)
        self.show_map(vehicle_data, stop_id)

    # rysowanie tabeli z odjazdami
    def show_table(self, parent_frame, data):
        for widget in parent_frame.winfo_children():
            widget.destroy()

        tree = ttk.Treeview(parent_frame, columns=list(data.columns), show='headings', height=8)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        for col in data.columns:
            tree.heading(col, text=col)
            tree.column(col, anchor=tk.CENTER)

        for _, row in data.iterrows():
            tree.insert('', tk.END, values=list(row))

        scrollbar = ttk.Scrollbar(parent_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    # rysowanie mapy z pojazdami
    def show_map(self, vehicle_data, stop_id):
        # pobranie informacji o przystanku
        stop_id, stop_name, stop_lat, stop_lon, _ = self.stop_data.loc[self.stop_data['stop_id'] == stop_id].values[0]

        # usuwanie starych pinezek
        self.map_view.delete_all_marker()

        # ustawienie pozycji mapy na przystanek
        self.map_view.set_position(stop_lat, stop_lon)
        self.map_view.set_zoom(14)

         # rysowanie pinezek dla pojazdów
        for _, row in vehicle_data.iterrows():
            self.map_view.set_marker(row['latitude'], row['longitude'], text=f'{row['route_long_name']} {row['trip_headsign']}\n{row['license_plate']}\n{row['distance']}m do {row['stop_name']}', marker_color_outside='blue', marker_color_circle='white')

        # rysowanie pinezki przystanku
        self.map_view.set_marker(stop_lat, stop_lon, text=stop_name, marker_color_outside='blue', marker_color_circle='yellow')

    # odświeza dane dla mapy
    def refresh_data(self):
        self.fetch_stop_data()
        self.after(self.refresh_interval, self.refresh_data)

    # zakładka z opóźnieniami
    def setup_tab2(self):
        # ramka dla menu wyboru
        frame_top = ttk.Frame(self.tab2)
        frame_top.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        frame_mid = ttk.Frame(self.tab2)
        frame_mid.pack(side=tk.TOP, fill=tk.X)

        # ramka z wykresami
        self.scroll_canvas = tk.Canvas(self.tab2)
        self.scroll_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar_y = ttk.Scrollbar(self.tab2, orient=tk.VERTICAL, command=self.scroll_canvas.yview)
        scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.scroll_canvas.configure(yscrollcommand=scrollbar_y.set)

        self.canvas_frame = ttk.Frame(self.scroll_canvas)
        self.scroll_canvas.create_window((0, 0), window=self.canvas_frame, anchor="nw")

        self.canvas_frame.bind("<Configure>", lambda e: self.scroll_canvas.configure(scrollregion=self.scroll_canvas.bbox("all")))

        # wybieranie numeru linii
        self.route_var = tk.StringVar()
        self.route_data = fetch_route_names()
        ttk.Label(frame_top, text="Linia:").pack(side=tk.LEFT, padx=5, pady=5)
        self.route_menu = ttk.Combobox(frame_top, textvariable=self.route_var, state="readonly")
        self.route_menu['values'] = self.route_data['route_long_name'].tolist()
        self.route_menu.pack(side=tk.LEFT, padx=5, pady=5)

        # wybieranie kierunku docelowego
        self.direction_var = tk.StringVar()
        ttk.Label(frame_top, text="Kierunek:").pack(side=tk.LEFT, padx=5, pady=5)
        self.direction_menu = ttk.Combobox(frame_top, textvariable=self.direction_var, state="readonly")
        self.direction_menu.pack(side=tk.LEFT, padx=5, pady=5)
        self.route_menu.bind("<<ComboboxSelected>>", self.fetch_directions)

        # wybór daty i godziny
        self.date_var = tk.StringVar(value= datetime.now().strftime("%Y-%m-%d"))
        self.hour_var = tk.IntVar(value=datetime.now().hour)
        ttk.Label(frame_top, text="Data:").pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Entry(frame_top, textvariable=self.date_var, width=10).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Label(frame_top, text="Godzina:").pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Spinbox(frame_top, from_=0, to=23, textvariable=self.hour_var, width=3).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(frame_top, text="Pobierz kursy", command=self.fetch_trips).pack(side=tk.LEFT, padx=5, pady=5)

        # wybór przejazdu
        self.trip_var = tk.StringVar()
        ttk.Label(frame_mid, text="Select Trip ID:").pack(side=tk.LEFT, padx=5, pady=5)
        self.trip_menu = ttk.Combobox(frame_mid, textvariable=self.trip_var, state="readonly")
        self.trip_menu.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(frame_mid, text="Rysuj", command=self.plot_delay).pack(side=tk.LEFT, padx=5, pady=5)

    # na podstawie wybranej lini pobiera mozliwe kierunki
    def fetch_directions(self, event):
        route = self.route_var.get()
        destinations = fetch_destination_stops(route)['trip_headsign'].tolist()
        self.direction_menu['values'] = destinations
        self.direction_var.set(destinations[0] if destinations else "")

    # pobiera przejazdy
    def fetch_trips(self):
        route = self.route_var.get()
        headsign = self.direction_var.get()
        date = self.date_var.get()
        hour = self.hour_var.get()
        trips = fetch_trips_by_headsign_and_time(headsign, date, hour)
        trip_ids = trips["trip_id"].tolist()
        self.trip_menu['values'] = trip_ids
        self.trip_var.set(trip_ids[0] if trip_ids else "")

    # rysuje wykres opóźnienia
    def plot_delay(self):
        delays = fetch_delays(self.trip_var.get(), self.date_var.get(), self.hour_var.get())
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.plot(delays["stop_name"], delays["delay"], marker='o', linestyle='-', color='blue')
        ax.set_xlabel("Nazwa przystanku")
        ax.set_ylabel("Opóźnienie [s]")
        ax.set_title(f"Opóźnienie względem przystanku (Trip ID: {delays['trip_id'].iloc[0]}) (Próbka czasu: {delays['timestamp'].iloc[-1]})")
        ax.grid(True, linestyle='--', alpha=0.5)
        plt.xticks(rotation=90, ha='right')

        canvas = FigureCanvasTkAgg(fig, master=self.canvas_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True, pady=10) 
        canvas.draw()

    


if __name__ == "__main__":
    app = MainApp()
    app.after(1000, app.refresh_data)
    app.mainloop()
