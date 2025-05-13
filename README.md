# Data Engineer Test

### Overview
The Central Data & Analytics Org needs to create a service that accepts a user id along with latitude and longitude coordinates and perform a geographic lookup against the [government weather API](https://www.weather.gov/documentation/services-web-api). Using that API we need the following information:
* The Forecast
* Tomorrow's Hourly Forecast

We also need the following derived data points:
* Hourly temperature as a ratio to the forecasted time period temperature
* Boolean feature indicating if the wind speed for an hour is above the daily average
* Latitude distance from forecast area
* Longitute distance from forecast area
* Chance of precipitation for that hour

After the data has been processed, it should written to a postgres database.

### Project setup
Requirements:
* [Docker](https://www.docker.com/)

This project stands up a lambda (python3.10) and a postgres database within docker compose. The schema of the postgres database can be found in the `db` directory. You shouldn't have to change much in the `Dockerfile` or `docker-compose.yml` unless you would like to use a different programming language. Otherwise most of your work will be in the `consumer/lib` directory. If you need any additional dependencies, set them in the `requirements.txt` and they will be installed when the image is built.

To start the stack run the following: `docker compose up --build`. You can now ping the lambda at `http://0.0.0.0:9001/2015-03-31/functions/function/invocations`. The lambda expects a payload containing user id, latitude and longitude. You can run the `run.sh` script to make 3 example requests to the lambda.

As you make any changes to the lambda you can always update the running image but running: `docker compose restart consumer`. To shut down the entire project run: `docker compose down -v`

While the containers are running if you need to connect to postgres, the user, databasename and password are all `test` (can be seen in the docker compose).
