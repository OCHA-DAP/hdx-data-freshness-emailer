FROM mcarans/hdx-data-freshness

WORKDIR /srv

RUN pip --no-cache-dir install hdx-data-freshness-emailer

CMD ["python3", "-m", "hdx.freshness.emailer.app"]
