FROM public.ecr.aws/unocha/hdx-data-freshness:1.8.9

WORKDIR /srv

COPY . .

RUN pip --no-cache-dir install -r docker-requirements.txt


CMD ["python3", "-m", "hdx.freshness.emailer.app"]
