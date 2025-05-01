""""Lambda consumer"""
import logging
from dataclasses import dataclass

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class UserCoordinate:
    user_id: int
    latitude: float
    longitude: float


def handler(event, context):
    user_coordinate = UserCoordinate(
        user_id = event["user_id"],
        latitude = event["latitude"],
        longitude = event["longitude"]
    )
    return {"status": 200}
