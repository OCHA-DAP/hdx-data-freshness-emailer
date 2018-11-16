FROM mcarans/hdx-data-freshness

MAINTAINER Michael Rans <rans@email.com>

RUN pip --no-cache-dir install hdx-data-freshness-emailer

CMD ["python3", "-m", "hdx.freshness.emailer"]
