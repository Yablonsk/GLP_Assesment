CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.user_coordinates (
  user_id INT NOT NULL,
  latitude FLOAT NOT NULL,
  longitude FLOAT NOT NULL,
  PRIMARY KEY (user_id)
);
