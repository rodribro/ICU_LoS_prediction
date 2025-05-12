### <b> Large Scale Data Science project </b>

<b> Instalação </b>

1) Instalem a versão do PostGreSQL version: psql (PostgreSQL) 14.17

2) Clonar repo e instalar dependências:
    * Façam git clone do projeto
    * No diretório do projeto criem um venv com o comando:  python3.12 -m venv venv
    * Ativem o environment: source venv/bin/activate
    * Para instalarem as dependências corram: pip install -r requirements.txt

<b> Project Goals </b>

* <b> Goal: </b> The goal of this project is to establish a Machine Learning pipeline that is able to efficently consume and process large amounts of data as well as to accurately predict the length of stay in ICU for a given patient based on the [MIMIC-III dataset](https://mimic.mit.edu/docs/iii/). To that end, we'll be using multiprocressing and parallel processing techniques to ease memory usage and time consumption.

* <b> Tech stack: </b> Python (Scikit-learn, Dask, PsycoPG2), PostGreSQL, Google Cloud Platform (BigQuery)

* <b> How to locally set-up the project: </b> TBA

NB: the file [Chart Events](https://mimic.mit.edu/docs/iii/tables/chartevents/) will not be included in the github repo due to its ~36GB size.




