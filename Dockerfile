FROM python:3

WORKDIR /app

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_REGION

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir ~/.aws
RUN echo " \
    [default] \
    aws_access_key_id = ${AWS_ACCESS_KEY_ID} \
    aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY} \
" > ~/.aws/credentials
RUN echo " \
    [default] \
    aws_secret_access_key = ${AWS_REGION} \
" > ~/.aws/config

CMD [ "python", "./__main__.py" ]