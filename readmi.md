# Real-Time Data Producers

This project is a real-time data producer system that uses various APIs to fetch data and send it to a Kafka server. The data producers are implemented in Python and use the Streamlit library for the user interface.
## Data Flow

The data flow in this project is as follows:

1. The data producers (`HespressProducer.py`, `TwitterProducer.py`, and `YoutubeProducer.py`) fetch data from their respective APIs.

2. The fetched data is sent to a Kafka server using the Kafka Python library.

3. The data consumers (`spark_streaming.py`) consume the data from the Kafka server using Spark Streaming.

4. The consumed data is processed and stored in MongoDB using the PyMongo library.

5. The stored data can be visualized using Streamlit by running the scripts in the `visualisations/` directory.

## Visualization with Streamlit

To visualize the data, follow these steps:

1. Make sure you have Streamlit installed. If not, you can install it using `pip install streamlit`.

2. Navigate to the `visualisations/` directory.

3. Run the desired Streamlit script, for example, `streamlit_app.py`, using the command `streamlit run streamlit_app.py`.

4. Open your web browser and go to the URL provided by Streamlit.

5. Explore the visualizations and interact with the data.

Note: Make sure you have the necessary dependencies installed for the data producers, Kafka, MongoDB, PySpark, and Docker as mentioned in the previous section.
## Project Structure

The project is structured as follows:

- `producers/`: Contains the Python scripts for the data producers. Each producer is implemented in a separate file:
    - `HespressProducer.py`
    - `TwitterProducer.py`
    - `YoutubeProducer.py`
    - `MainProducers.py`: This is the main script that runs the data producers.

- `costumer/`: Contains the scripts for the data consumers. The consumers are implemented in Spark Streaming and are run in a Jupyter notebook.

- `kafka-cluster/`: Contains the Docker Compose file for setting up the Kafka cluster.

- `mongodb-custe/`: Contains the Docker Compose file for setting up the MongoDB cluster and the Flask API.

- `pyspark-jupyter-lab/`: Contains the Dockerfile for setting up the PySpark Jupyter Lab.

- `spark-cluster/`: Contains the Docker Compose file for setting up the Spark cluster.

- `visualisations/`: Contains the scripts for visualizing the data.

## How to Run

1. Start the Kafka and MongoDB clusters by running the Docker Compose files in the `kafka-cluster/` and `mongodb-custe/` directories.

2. Start the PySpark Jupyter Lab by building and running the Dockerfile in the `pyspark-jupyter-lab/` directory.

3. Run the `MainProducers.py` script in the `producers/` directory. This will start the data producers. You can choose which producers to start and specify the keywords for the Hespress and Twitter producers and the video ID for the YouTube producer.

4. Run the `spark_streaming.py` script in the `costumer/` directory. This will start the data consumers.

5. Visualize the data by running the scripts in the `visualisations/` directory.

**Note:** Please replace the YouTube API key in the `MainProducers.py` script with your own API key.

## Dependencies

- Python
- Streamlit
- Kafka
- MongoDB
- PySpark
- Docker