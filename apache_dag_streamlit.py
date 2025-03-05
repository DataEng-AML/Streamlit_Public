# Test Streamlit App
import streamlit as st
import pandas as pd
import pydeck as pdk
import numpy as np

# UK cities data
uk_cities = [
    ("London", 51.5074, -0.1278, 300),
    ("Birmingham", 52.4862, -1.8904, 158),
    ("Manchester", 53.4808, -2.2426, 20),
    ("Edinburgh", 55.9533, -3.1883, 258),
    ("Glasgow", 55.8642, -4.2518, 30),
    ("Liverpool", 53.4084, -2.9916, 188),
    ("Leeds", 53.8008, -1.5491, 22),
    ("Sheffield", 53.3811, -1.4701, 28),
    ("Bristol", 51.4545, -2.5879, 17),
    ("Newcastle", 54.9783, -1.6178, 23),
    ("Cardiff", 51.4816, -3.1791, 198),
    ("Belfast", 54.5973, -5.9301, 21),
    ("Nottingham", 52.9548, -1.1581, 16),
    ("Leicester", 52.6369, -1.1398, 148),
    ("Coventry", 52.4068, -1.5197, 13)
]

# Create DataFrame
data = pd.DataFrame(uk_cities, columns=['name', 'lat', 'lon', 'forest_coverage'])
data['forest_size'] = data['forest_coverage'] * 100  # Scale for visibility

# Color scale function
def get_color(size):
    r = int(255 * (1 - size/data['forest_coverage'].max()))
    g = int(255 * (size/data['forest_coverage'].max()))
    return [r, g, 0, 200]

data['color'] = data['forest_coverage'].apply(get_color)

st.title("UK Forest Coverage Map")

# Sidebar controls
st.sidebar.header("Map Controls")
zoom_level = st.sidebar.slider("Zoom Level", 5, 15, 6)
pitch = st.sidebar.slider("Pitch", 0, 60, 40)

# Create the map
st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
        latitude=54.5,  # Centered on UK
        longitude=-4.0,
        zoom=zoom_level,
        pitch=pitch,
    ),
    layers=[
        pdk.Layer(
            'ScatterplotLayer',
            data=data,
            get_position='[lon, lat]',
            get_color='color',
            get_radius='forest_size',
            pickable=True,
            auto_highlight=True,
            opacity=0.8,
        ),
    ],
    tooltip={
        'html': '<b>{name}</b><br>Forest Coverage: {forest_coverage}%',
        'style': {
            'backgroundColor': 'steelblue',
            'color': 'white'
        }
    }
))

# Display data table
st.subheader("Forest Coverage Data")
st.dataframe(data[['name', 'forest_coverage']])

# Add a chart
st.subheader("Forest Coverage by City")
st.bar_chart(data.set_index('name')['forest_coverage'])
