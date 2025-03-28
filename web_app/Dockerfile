# A dockerfile must always start by importing the base image.
# We use the keyword 'FROM' to do that.
# In our example, we want import the python image.
# So we write 'python' for the image name and 'latest' for the version.
FROM python:3.10.12
WORKDIR /home/web_app_project_root


#COPY flask_app.py models.py map_configuration.py requirements.txt airflow_dags/* processed/html_files/* ./
#COPY . .
RUN mkdir -p /home/web_app_project_root/web_app
COPY web_app  /home/web_app_project_root/web_app
COPY config.py db_connections.py dev_config.yaml ./
COPY requirements.txt ./
#COPY requirements.txt /home
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r /home/web_app_project_root/requirements.txt

# We need to define the command to launch when we are going to run the image.
# We use the keyword 'CMD' to do that.
#CMD [ "python", "flask_app.py" ]
CMD ["sh", "-c", "python -m web_app"]
