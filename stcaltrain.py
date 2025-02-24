import requests
import pandas as pd
import streamlit as st
import pytz
import datetime
from streamlit_extras.badges import badge
from functions.ct_functions import (
    get_schedule,
    assign_train_type,
    is_northbound,
)
from geopy.distance import geodesic
import requests
import json

st.set_page_config(page_title="Caltrain Platform", page_icon="🚆", layout="centered")


@st.cache_resource(ttl="60s")
def ping_train() -> dict:
    # URL for the 511 Transit API
    url = f"https://api.511.org/transit/VehicleMonitoring?api_key={st.secrets['511_key']}&agency=CT"

    # Making the request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Decode using utf-8-sig to handle UTF-8 BOM
        decoded_content = response.content.decode("utf-8-sig")

        # Parse the decoded content into JSON
        data = json.loads(decoded_content)

    else:
        return False

    if data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"].get("VehicleActivity") is None:
        return False
    else:
        return data


data = ping_train()


def create_caltrain_dfs(data: dict) -> pd.DataFrame:
    """Ping 511 API and reformat the data"""
    trains = []
    for train in data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"]["VehicleActivity"]:
        train_obj = train["MonitoredVehicleJourney"]

        if train_obj.get("OnwardCalls") is None:
            continue

        next_stop_df = pd.DataFrame(
            [
                [
                    train_obj["MonitoredCall"]["StopPointName"],
                    train_obj["MonitoredCall"]["StopPointRef"],
                    train_obj["MonitoredCall"]["AimedArrivalTime"],
                    train_obj["MonitoredCall"]["ExpectedArrivalTime"],
                ]
            ],
            columns=["stop_name", "stop_id", "aimed_arrival_time", "expected_arrival_time"],
        )
        destinations_df = pd.DataFrame(
            [
                [
                    stop["StopPointName"],
                    stop["StopPointRef"],
                    stop["AimedArrivalTime"],
                    stop["ExpectedArrivalTime"],
                ]
                for stop in train_obj["OnwardCalls"]["OnwardCall"]
            ],
            columns=["stop_name", "stop_id", "aimed_arrival_time", "expected_arrival_time"],
        )
        destinations_df = pd.concat([next_stop_df, destinations_df])
        destinations_df["id"] = train_obj["VehicleRef"]
        destinations_df["origin"] = train_obj["OriginName"]
        destinations_df["origin_id"] = train_obj["OriginRef"]
        destinations_df["direction"] = train_obj["DirectionRef"] + "B"
        destinations_df["line_type"] = train_obj["PublishedLineName"]
        destinations_df["destination"] = train_obj["DestinationName"]
        destinations_df["train_longitude"] = train_obj["VehicleLocation"]["Longitude"]
        destinations_df["train_latitude"] = train_obj["VehicleLocation"]["Latitude"]

        destinations_df = destinations_df[
            [
                "id",
                "origin",
                "origin_id",
                "direction",
                "line_type",
                "destination",
                "stop_name",
                "stop_id",
                "aimed_arrival_time",
                "expected_arrival_time",
                "train_longitude",
                "train_latitude",
            ]
        ]
        destinations_df["stops_away"] = destinations_df.index
        trains.append(destinations_df)
    trains_df = pd.concat(trains)

    # Change to the correct types
    trains_df["aimed_arrival_time"] = pd.to_datetime(trains_df["aimed_arrival_time"])
    trains_df["expected_arrival_time"] = pd.to_datetime(trains_df["expected_arrival_time"])
    trains_df["train_longitude"] = trains_df["train_longitude"].astype(float)
    trains_df["train_latitude"] = trains_df["train_latitude"].astype(float)
    trains_df["stop_id"] = trains_df["stop_id"].astype(float)
    trains_df["origin_id"] = trains_df["origin_id"].astype(float)

    # Import the stop_ids and add their coordinates to the dataframe
    stop_ids = pd.read_csv("stop_ids.csv")

    # Combine the stop IDs
    sb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id", right_on="stop1", how="inner")
    nb_trains_df = pd.merge(trains_df, stop_ids, left_on="stop_id", right_on="stop2", how="inner")
    trains_df = pd.concat([sb_trains_df, nb_trains_df])

    trains_df["distance"] = trains_df.apply(
        lambda x: geodesic((x["train_latitude"], x["train_longitude"]), (x["lat"], x["lon"])).miles,
        axis=1,
    )
    trains_df["distance"] = trains_df["distance"].round(1).astype("str") + " mi"
    trains_df["Departure Time"] = trains_df["expected_arrival_time"]
    trains_df["Current Time"] = datetime.datetime.now(pytz.timezone("UTC"))
    trains_df["ETA"] = trains_df["Departure Time"] - trains_df["Current Time"]
    trains_df["Train #"] = trains_df["id"]
    trains_df["Direction"] = trains_df["direction"]
    return trains_df


def clean_up_df(data: pd.DataFrame) -> pd.DataFrame:
    """Clean up the dataframe for display"""
    # Filter for desired columns
    data = data[["Train #", "Train Type", "Departure Time", "ETA", "distance", "stops_away"]]
    data["ETA"] = data["ETA"].apply(lambda x: int(x.total_seconds() / 60))
    data["ETA"] = data["ETA"].astype("str") + " min"

    # data["ETA"] = data["ETA"].apply(lambda x: f"{int(x // 60)} hr {int(x % 60)} min")

    # Rename the columns
    data.columns = [
        "Train #",
        "Train Type",
        "Departure Time",
        "ETA",
        "Distance to Station",
        "Stops Away",
    ]

    data = data.T
    data.columns = data.iloc[0]
    data = data.drop(data.index[0])

    return data


if data is not False:
    caltrain_data = create_caltrain_dfs(data)
else:
    caltrain_data = False

st.title("🚊 Caltrain Platform 🚂")
caltrain_stations = pd.read_csv("stop_ids.csv")
col1, col2 = st.columns([2, 1])

col1.markdown(
    """
    Track when the next trains leave from your station and where they are right now.
    """
)

api_working = True if type(caltrain_data) == pd.DataFrame else False
scheduled = False

# Allow switch between live data and scheduled data
if api_working:
    display = col1.radio(
        "Show trains",
        ["Live", "Scheduled"],
        horizontal=True,
        help="Live shows only trains that have already left the station",
    )
    schedule_chosen = True
else:
    display = col1.radio(
        "Show trains",
        ["Live", "Scheduled"],
        horizontal=True,
        help="Live shows only trains that have already left the station",
        index=1,
        disabled=True,
    )
    schedule_chosen = False

if display == "Scheduled":
    scheduled = True
    col1, col2 = st.columns([2, 1])
    if schedule_chosen:
        col1.info("📆 Pulling the current schedule from the Caltrain website...")
    else:
        col1.error(
            "❌ Caltrain Live Map API is currently down. Pulling the current schedule from the Caltrain website instead..."
        )

    # Get southbound trains (Millbrae to Palo Alto)
    millbrae_to_palo = get_schedule("southbound", "Millbrae", "Palo Alto")

    # Get northbound trains (Palo Alto to Millbrae)
    palo_to_millbrae = get_schedule("northbound", "Palo Alto", "Millbrae")

    # Display the dataframes split by Train #, Scheduled Departure, Current Stop and the other columns
    # Going to Work section first
    col1.subheader("Going to Work (Millbrae → Palo Alto)")
    sb_data = millbrae_to_palo.drop("Direction", axis=1).reset_index(drop=True)
    sb_data.index = sb_data.index + 1
    sb_data = sb_data.T
    sb_data.columns = sb_data.iloc[0]
    sb_data = sb_data.drop(sb_data.index[0])
    col1.dataframe(sb_data, use_container_width=True)

    # Going Home section second
    col1.subheader("Going Home (Palo Alto → Millbrae)")
    nb_data = palo_to_millbrae.drop("Direction", axis=1).reset_index(drop=True)
    nb_data.index = nb_data.index + 1
    nb_data = nb_data.T
    nb_data.columns = nb_data.iloc[0]
    nb_data = nb_data.drop(nb_data.index[0])
    col1.dataframe(nb_data, use_container_width=True)

else:
    col1.info("✅ Caltrain API is up and running")

    caltrain_data["Train Type"] = caltrain_data["Train #"].apply(lambda x: assign_train_type(x))

    caltrain_data["Departure Time"] = (
        pd.to_datetime(caltrain_data["Departure Time"])
        .dt.tz_convert("US/Pacific")
        .dt.strftime("%I:%M %p")
    )


    # Function to check if a train passes through both stations
    def train_passes_through(train_id, start_station, end_station, direction):
        # Get all stops for this train
        train_stops = caltrain_data[caltrain_data["id"] == train_id]["stopname"].unique()

        # Check if both stations are in the stop list
        if start_station in train_stops and end_station in train_stops:
            # Verify it's going in the right direction
            train_direction = caltrain_data[caltrain_data["id"] == train_id]["direction"].iloc[0]
            return train_direction == direction
        return False


    # Find trains that go from Millbrae to Palo Alto (southbound)
    valid_sb_trains = []
    for train_id in caltrain_data["id"].unique():
        if train_passes_through(train_id, "Millbrae", "Palo Alto", "SB"):
            valid_sb_trains.append(train_id)

    # Find trains that go from Palo Alto to Millbrae (northbound)
    valid_nb_trains = []
    for train_id in caltrain_data["id"].unique():
        if train_passes_through(train_id, "Palo Alto", "Millbrae", "NB"):
            valid_nb_trains.append(train_id)

    # Southbound trains (Millbrae to Palo Alto) - Going to Work
    col1.subheader("Work 💪(M → P)")
    sb_trains = caltrain_data[caltrain_data["id"].isin(valid_sb_trains)]
    sb_trains = sb_trains[sb_trains["stopname"] == "Millbrae"]
    sb_trains = sb_trains.sort_values(by=["ETA"])

    if len(sb_trains) == 0:
        col1.info("No trains")
    else:
        col1.dataframe(clean_up_df(sb_trains), use_container_width=True)

    # Northbound Trains (Palo Alto to Millbrae) - Going Home
    col1.subheader("Home 😄(P → M)")
    nb_trains = caltrain_data[caltrain_data["id"].isin(valid_nb_trains)]
    nb_trains = nb_trains[nb_trains["stopname"] == "Palo Alto"]
    nb_trains = nb_trains.sort_values(by=["ETA"])

    if nb_trains.empty:
        col1.info("No trains")
    else:
        col1.dataframe(clean_up_df(nb_trains), use_container_width=True)

if col1.button("Refresh Data"):
    st.experimental_rerun()

# Definitions
col1.markdown("---")
col1.subheader("Definitions")
col1.markdown(
    """
1. **Train Number** - The train ID. The first digit indicates the train type.
2. **Train Type** - Local trains make all stops. Limited and Bullet make fewer.
3. **Departure Time** - The scheduled departure time of the train from the origin station.
4. **ETA** - The estimated number of minutes before the train arrives.
5. **Distance to Station** - The distance from the train to the origin station.
6. **Stops Away** - The number of stops until the train reaches the origin station.
"""
)

col1.subheader("About")
col1.markdown(
    """
- This app pulls _real-time_ data from the [511 API](https://511.org/open-data). It was created to solve the issue of arriving at the Caltrain station while the train is behind schedule. This app will tell you when the next train is leaving, and about how long it will take to arrive at the station.

- **Note:** If the caltrain API is down or there aren't any trains moving, then the app will pull the current schedule from the Caltrain website instead.

- This app specifically shows:
  - Going to Work: Trains from Millbrae to Palo Alto (formerly Southbound)
  - Going Home: Trains from Palo Alto to Millbrae (formerly Northbound)
"""
)

col1, col2 = st.columns([2, 1])
with col1:
    st.markdown("---")

col1, col2 = st.columns([1, 1])
with col1:
    badge("twitter", "TYLERSlMONS", "https://twitter.com/TYLERSlMONS")
with col2:
    badge("github", "tyler-simons/caltrain", "https://github.com/tyler-simons/caltrain")