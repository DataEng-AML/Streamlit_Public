import streamlit as st
import folium
import math
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from folium.plugins import HeatMap
from streamlit_folium import st_folium

# Streamlit UI
st.title("UK Local Authority Data Dashboard")
st.subheader("Analysis of Manufacturing Data Across Local Authorities")
st.write("This dashboard provides a visualization of manufacturing activity across the UK local authorities, including a ranked bar chart and an interactive map.")

# File upload for datasets
st.write("### Upload Data Files")
data_file = st.file_uploader("Upload CSV Data File", type=["csv"])
geojson_file = st.file_uploader("Upload GeoJSON File", type=["geojson"])

if data_file and geojson_file:
    # Load the data
    data = pd.read_csv(data_file, header=0)
    
    # Load the UK local authority boundaries shapefile
    gdf = gpd.read_file(geojson_file)
    
    # Merge the data with the geopandas dataframe
    merged = gdf.merge(data, left_on='LAD24CD', right_on='area code')
    
    # Dropdown to select category
    category_options = ["Total specific manufacturing", "Total specific and related manufacturing"]
    category = st.selectbox("Select Category to Visualize:", category_options)
    
    # Aggregate data by ITL2 code
    grouped_data = merged.groupby("ITL2 code")[category].sum().reset_index()
    
    # Create two columns
    col1, col2 = st.columns([1, 2])
    
    # Bar chart of top 10 values
    with col1:
        st.write("### Top 10 ITL2 Regions by Manufacturing Value")
        top_10 = grouped_data.nlargest(10, category)
        fig, ax = plt.subplots()
        ax.barh(top_10['ITL2 code'], top_10[category], color='skyblue')
        ax.set_xlabel("Manufacturing Value")
        ax.set_ylabel("ITL2 Code")
        ax.invert_yaxis()
        st.pyplot(fig)
    
    # Interactive map
    with col2:
        st.write("### Interactive Map of Manufacturing Data")
        m = folium.Map(location=[55.3781, -3.4360], zoom_start=6)
        
        # Add circles for the selected category
        for _, row in merged.iterrows():
            value = row[category]
            if value > 0:
                radius = math.sqrt(value) * 0.2  # Adjust the factor to control circle size
                folium.CircleMarker(
                    location=[row['geometry'].centroid.y, row['geometry'].centroid.x],
                    radius=radius,
                    color='red',
                    fill=True,
                    fill_color='red',
                    fill_opacity=0.7,
                    popup=f"{row['local authority: district / unitary (as of April 2023)']}<br>{category}: {value}",
                    tooltip=f"{row['local authority: district / unitary (as of April 2023)']}<br>{category}: {value}"
                ).add_to(m)
        
        folium.LayerControl().add_to(m)
        st_folium(m, width=700, height=500)
else:
    st.write("Please upload both the data CSV file and the GeoJSON file to proceed.")
