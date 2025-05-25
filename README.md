### <b> Large Scale Data Science project </b>


<b> Project Goals </b>

* <b> Goal: </b> The goal of this project is to establish a Machine Learning pipeline that is able to efficently consume and process large amounts of data as well as to accurately predict the length of stay in ICU for a given patient based on the [MIMIC-III dataset](https://mimic.mit.edu/docs/iii/). To that end, we'll be using multiprocressing and parallel processing techniques to ease memory usage and time consumption.

* <b> Tech stack: </b> Python (PySpark), Google Cloud Platform (BigQuery)

* <b> How to locally set-up the project: </b> 
    * In the ICU_LOS_PREDICTION directory run the following code in the terminal: <code> python3.12 -m venv venv </code> (creates a python virtual envrionment)
    * Activate the environment by running: <code> source venv/bin/activate </code>
    * Afterwards run:  <code> pip install -r requirements.txt </code> (install the required dependencies)

Note: The files [Chart Events](https://mimic.mit.edu/docs/iii/tables/chartevents/) and [DateTimeEvent](https://mimic.mit.edu/docs/iii/tables/datetimeevents/) will not be included in the github repo due to its ~36GB size. (Might add them later via Git's LFS)




