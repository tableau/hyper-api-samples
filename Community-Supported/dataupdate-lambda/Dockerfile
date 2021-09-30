FROM public.ecr.aws/lambda/python:3.8

# remove when merged
COPY tableauserverclient-0.15.0+41.gb8d4a01-py2.py3-none-any.whl ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}
COPY app.py ${LAMBDA_TASK_ROOT}


RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install tableauserverclient-0.15.0+41.gb8d4a01-py2.py3-none-any.whl
#^add tsc to reqs once merged

COPY . .

CMD [ "app.handler" ]