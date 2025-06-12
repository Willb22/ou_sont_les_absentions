# A dockerfile must always start by importing the base image.
# We use the keyword 'FROM' to do that.
# In our example, we want import the python image.
# So we write 'python' for the image name and 'latest' for the version.
FROM python:3.10.12
#WORKDIR /home/project_root/process_raw_data
WORKDIR /home/project_root
RUN mkdir -p /home/project_root/process_data

COPY process_data  /home/project_root/process_data
COPY config.py db_connections.py dev_config.yaml requirements.txt ./

#COPY flask_app.py models.py map_configuration.py requirements.txt airflow_dags/* processed/html_files/* ./
#COPY . .
#COPY ../config.py ../db_connections.py ../dev_config.yaml ../requirements.txt /home/project_root
#COPY requirements.txt /home
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r /home/project_root/requirements.txt

# We need to define the command to launch when we are going to run the image.
# We use the keyword 'CMD' to do that.
#CMD ["sh", "-c", "python -m process_data.raw_data_download && python -m process_data.feed_data_to_postgresql"]
CMD ["sh", "-c", "python -m process_data"]

#CMD [ "python", "raw_data_download.py" ]
#CMD [ "python", "feed_data_to_postgresql.py" ]