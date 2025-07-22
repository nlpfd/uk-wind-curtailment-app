import streamlit as st
from lib.gcp_db_utils import read_data
from datetime import datetime

# Constants for data query date range
MIN_DATE = "2020-01-01 00:00:00"
MAX_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Cache data query results for 10 minutes to reduce DB load
@st.experimental_memo(ttl=600)
def get_data(start_time=MIN_DATE, end_time=MAX_DATE):
    return read_data(start_time=start_time, end_time=end_time)

def main():
    st.title("Wind Curtailment Dashboard")

    # Get data (cached)
    df = get_data()

    # Display a preview of the data
    st.write("Here is a preview of the data:")
    st.dataframe(df.head())

if __name__ == "__main__":
    main()
