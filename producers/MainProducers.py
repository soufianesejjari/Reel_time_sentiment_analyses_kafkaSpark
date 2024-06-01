import streamlit as st
from concurrent.futures import ThreadPoolExecutor
from HespressProducer import HespressProducer
from TwitterProducer import TwitterProducer
from YoutubeProducer import YoutubeProducer

class MainProducer:
    def __init__(self, server, api_key):
        self.server = server
        self.api_key = "api key"  # replace with your YouTube API k

    def run_producers(self, hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube):
        if start_hespress:
            hespress_producer = HespressProducer(self.server)
        if start_twitter:
            twitter_producer = TwitterProducer(self.server)
        if start_youtube:
            youtube_producer = YoutubeProducer(self.server, api_key=self.api_key)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            if start_hespress:
                futures.append(executor.submit(hespress_producer.run, hespress_keyword))
            if start_twitter:
                futures.append(executor.submit(twitter_producer.run, hespress_keyword))
            if start_youtube:
                futures.append(executor.submit(youtube_producer.run, youtube_video_id, hespress_keyword))
            for future in futures:
                try:
                    result = future.result()
                except Exception as e:
                    st.error(f"Exception occurred: {e}")

def start_producers(server, api_key, hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube):
    main_producer = MainProducer(server, api_key)
    main_producer.run_producers(hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube)
    st.success(f"Producers started successfully for Hespress: {hespress_keyword}, YouTube: {youtube_video_id}")

def main():
    st.title("Real-Time Data Producers")

    server = st.text_input("Kafka Server", "localhost:9092")
    api_key = st.text_input("YouTube API Key","Api key")

    if "keyword_sets" not in st.session_state:
        st.session_state.keyword_sets = []

    st.header("Add Keyword Set")
    hespress_keyword = st.text_input("Keyword for Hespress and Twitter")
    youtube_video_id = st.text_input("YouTube Video ID")

    start_hespress = st.checkbox("Start Hespress Producer", value=True)
    start_twitter = st.checkbox("Start Twitter Producer", value=True)
    start_youtube = st.checkbox("Start YouTube Producer", value=True)

    if st.button("Add Keyword Set"):
        if not hespress_keyword or not youtube_video_id:
            st.error("Please fill in all fields")
        else:
            st.session_state.keyword_sets.append((hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube))
            st.success("Keyword set added")

    st.header("Start Producers")

    if st.button("Start All Producers"):
        if not st.session_state.keyword_sets:
            st.error("No keyword sets to start")
        else:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube in st.session_state.keyword_sets:
                    futures.append(executor.submit(start_producers, server, api_key, hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube))
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        st.error(f"Exception occurred: {e}")

    st.header("Keyword Sets")
    for i, (hespress_keyword, youtube_video_id, start_hespress, start_twitter, start_youtube) in enumerate(st.session_state.keyword_sets):
        st.write(f"Set {i+1}: Hespress: {hespress_keyword}, YouTube: {youtube_video_id}, Start Hespress: {start_hespress}, Start Twitter: {start_twitter}, Start YouTube: {start_youtube}")

if __name__ == "__main__":
    main()
