# A dockerfile must always start by importing the base image.
# We use the keyword 'FROM' to do that.

FROM python:3.10.12
WORKDIR /home/web_app_project_root

RUN mkdir -p /home/web_app_project_root/web_app
COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r /home/web_app_project_root/requirements.txt

# We need to define the command to launch when we are going to run the image.
# We use the keyword 'CMD' to do that.
CMD ["sh", "-c", "python -m web_app"]
