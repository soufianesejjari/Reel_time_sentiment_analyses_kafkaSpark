import streamlit as st
import pandas as pd
import altair as alt
from collections import Counter
import re
import requests

# API endpoint URL
api_url = "http://localhost:5000/"

# Fetch data from the API
response = requests.get(api_url)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
else:
    st.error("Failed to fetch data from the API.")
    st.stop()

# Convert the date column to datetime
df["date"] = pd.to_datetime(df["date"])

# Streamlit main page
st.title("Sentiment Analysis Report")

# Sidebar filters
st.sidebar.header("Filter Options")
source_filter = st.sidebar.multiselect(
    "Select Source(s):", options=df["source"].unique(), default=df["source"].unique()
)

sentiment_filter = st.sidebar.multiselect(
    "Select Sentiment(s):",
    options=df["sentiment"].unique(),
    default=["positive", "negative", "neutral"],
)

date_filter = st.sidebar.date_input(
    "Select Date Range:",
    value=[pd.to_datetime(df["date"].min()), pd.to_datetime(df["date"].max())],
)

# Apply filters
filtered_df = df[
    (df["source"].isin(source_filter))
    & (df["sentiment"].isin(sentiment_filter))
    & (df["date"] >= pd.to_datetime(date_filter[0]))
    & (df["date"] <= pd.to_datetime(date_filter[1]))
]

# Display filtered data
st.header("Filtered Data")
st.dataframe(filtered_df)

# Display sentiment distribution chart
st.header("Sentiment Distribution")
sentiment_chart = (
    alt.Chart(filtered_df)
    .mark_bar()
    .encode(x="sentiment", y="count()", color="sentiment")
    .properties(width=600, height=400)
)
st.altair_chart(sentiment_chart)

# Display source distribution chart
st.header("Source Distribution")
source_chart = (
    alt.Chart(filtered_df)
    .mark_bar()
    .encode(x="source", y="count()", color="source")
    .properties(width=600, height=400)
)
st.altair_chart(source_chart)

# Line chart for sentiment change over time
st.header("Sentiment Change Over Time")

# Calculate percentage of positive sentiments for each source over time
sentiment_counts = (
    df.groupby(["date", "source", "sentiment"]).size().unstack(fill_value=0)
)
sentiment_counts["total"] = sentiment_counts.sum(axis=1)
sentiment_counts["percentage_happiness"] = (
    sentiment_counts["positive"] / sentiment_counts["total"] * 100
)

sentiment_counts.reset_index(inplace=True)
sentiment_counts_melted = pd.melt(
    sentiment_counts, id_vars=["date", "source"], value_vars=["percentage_happiness"]
)

line_chart = (
    alt.Chart(sentiment_counts_melted)
    .mark_line()
    .encode(
        x="date:T",
        y="value:Q",
        color="source:N",
        tooltip=["date:T", "source:N", alt.Tooltip("value:Q", format=".2f")],
    )
    .properties(width=800, height=500)
)

st.altair_chart(line_chart)

# Display most frequently used words in comments
st.header("Most Frequently Used Words")


def get_most_common_words(comments, num_words=10):
    all_comments = " ".join(comments)
    all_comments = re.sub(r"[^\w\s]", "", all_comments).lower()
    words = all_comments.split()
    most_common_words = Counter(words).most_common(num_words)
    return pd.DataFrame(most_common_words, columns=["word", "count"])


most_common_words_df = get_most_common_words(filtered_df["comment"])

word_chart = (
    alt.Chart(most_common_words_df)
    .mark_bar()
    .encode(x="count", y=alt.Y("word", sort="-x"), color="count")
    .properties(width=600, height=400)
)
st.altair_chart(word_chart)

# Add text input for searching comments
st.header("Search Comments")
search_term = st.text_input("Enter search term:")
if search_term:
    search_results = filtered_df[
        filtered_df["comment"].str.contains(search_term, case=False)
    ]
    st.dataframe(search_results)
else:
    st.write("Enter a search term to filter comments.")

# Display raw data
st.header("Raw Data")
st.write(df)
