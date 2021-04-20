# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_bigquery]
# [START gae_python3_bigquery]
import concurrent.futures

import flask
from google.cloud import bigquery

app = flask.Flask(__name__)


@app.route("/")
def main():
    return flask.render_template("index.html")


@app.route("/result1")
def result1():
    bigquery_client = bigquery.Client()
    query_job = bigquery_client.query(
        """
        SELECT time_ref,sum(case when account in ('Exports','Imports') then value end) trade_value 
        FROM `flaskforum-311009.a1_2.gsquarterlySeptember20` 
        group by time_ref order by sum(case when account in ('Exports','Imports') then value end) desc
        LIMIT 10
    """
    )
    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result1.html", results=results)


@app.route("/result2")
def result2():
    bigquery_client = bigquery.Client()
    query_job = bigquery_client.query(
        """
        select b.country_label, a.product_type, a.status, a.trade_deficit_value
        from
        (
        SELECT country_code,product_type,status,sum(case when account = 'Imports' then value
            when account = 'Exports' then -value
            end) trade_deficit_value
        FROM `flaskforum-311009.a1_2.gsquarterlySeptember20` 
        where time_ref between '201401' and '201612'
        and product_type='Goods'
        and status='F'
        group by country_code,product_type,status 
        ) a
        inner join `flaskforum-311009.a1_2.country_classification` b
        on a.country_code=b.country_code
        order by a.trade_deficit_value desc
        limit 50
            """
    )
    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result2.html", results=results)


@app.route("/result3")
def result3():
    bigquery_client = bigquery.Client()
    query_job = bigquery_client.query(
        """
        select b.service_label,
            sum( case when a.account = 'Imports' then - a.value when a.account = 'Exports' then a.value end) trade_surplus_value
        from `flaskforum-311009.a1_2.gsquarterlySeptember20` a
        inner join `flaskforum-311009.a1_2.services_classification` b on a.code=b.code
        where a.time_ref in 
        (
            SELECT time_ref
            FROM `flaskforum-311009.a1_2.gsquarterlySeptember20` 
            group by time_ref order by sum(case when account in ('Exports','Imports') then value end) desc
            LIMIT 10
        )
        and a.country_code in
        (
            SELECT country_code
            FROM `flaskforum-311009.a1_2.gsquarterlySeptember20` 
            where time_ref between '201401' and '201612'
            and product_type='Goods'
            and status='F'
            group by country_code,product_type,status 
            order by sum(case when account = 'Imports' then value when account = 'Exports' then -value end) desc limit 50
            
        ) group by  b.service_label order by sum(case when a.account = 'Imports' then -a.value when a.account = 'Exports' then a.value end) desc 
        limit 30
        """
    )
    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result3.html", results=results)


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run()
# [END gae_python3_bigquery]
# [END gae_python38_bigquery]
