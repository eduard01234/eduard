FROM python:3

ADD edge_app.py /

ADD requirements.txt /

RUN pip install -r /requirements.txt

CMD [ "python", "./edge_app.py" ]
