# main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from typing import List

from app import schemas, crud, nws_service, processing_service
from app.database import initialize_database, get_db_session, close_db_pool
from app.core.config import settings
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize database (create tables if they don't exist)
    await initialize_database()
    logger.info("Database initialized.")
    yield
    # Shutdown: Close database connection pool
    await close_db_pool()
    logger.info("Database connection pool closed.")

app = FastAPI(
    title="Weather Enrichment Microservice",
    description="Accepts coordinates, fetches weather data from NWS, enriches it, and stores it.",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/weather", response_model=schemas.WeatherRequestResponse, status_code=201)
async def get_and_store_weather_data(
    request_data: schemas.WeatherRequestCreate,
    db: AsyncSession = Depends(get_db_session)
):
    """
    Accepts user ID, latitude, and longitude.
    Retrieves weather data from the National Weather Service API.
    Performs data enrichment and transformation.
    Persists processed data to the database.
    """
    logger.info(f"Received weather request for user_id: {request_data.user_id}, lat: {request_data.latitude}, lon: {request_data.longitude}")

    # 1. Create initial weather request entry
    db_weather_request = await crud.create_weather_request(db, request_data)
    if not db_weather_request:
        logger.error("Failed to create weather request entry in DB.")
        raise HTTPException(status_code=500, detail="Could not create weather request entry.")

    try:
        # 2. Fetch NWS gridpoint metadata
        logger.info(f"Fetching NWS gridpoint for {request_data.latitude},{request_data.longitude}")
        grid_info = await nws_service.get_nws_gridpoint_info(request_data.latitude, request_data.longitude)
        
        await crud.update_weather_request_grid_info(db, db_weather_request.request_id, grid_info)
        logger.info(f"NWS Grid Info: Office={grid_info.grid_id}, X={grid_info.grid_x}, Y={grid_info.grid_y}")

        # 3. Fetch current and hourly forecasts
        logger.info(f"Fetching forecasts from NWS: {grid_info.forecast_url}, {grid_info.forecast_hourly_url}")
        current_forecast_data = await nws_service.get_nws_forecast(grid_info.forecast_url)
        hourly_forecast_data_raw = await nws_service.get_nws_forecast_hourly(grid_info.forecast_hourly_url)

        if not current_forecast_data or not hourly_forecast_data_raw:
            await crud.update_weather_request_status(db, db_weather_request.request_id, "FAILED", "Failed to fetch data from NWS API.")
            logger.error("Failed to fetch forecast data from NWS.")
            raise HTTPException(status_code=502, detail="Failed to retrieve data from National Weather Service API.")

        # 4. Process and Enrich Data
        logger.info("Processing and enriching NWS data.")
        # Current forecast (first period)
        processed_current_forecast = processing_service.process_current_forecast(
            current_forecast_data.get("properties", {}).get("periods", [{}])[0]
        )

        # Hourly forecast (next 24 hours)
        processed_hourly_forecasts = processing_service.process_and_enrich_hourly_forecasts(
            hourly_forecast_data_raw.get("properties", {}).get("periods", []),
            request_data.latitude,
            request_data.longitude
        )
        
        if not processed_hourly_forecasts: # Ensure we have hourly data to process
             await crud.update_weather_request_status(db, db_weather_request.request_id, "FAILED", "No hourly forecast periods found or processed.")
             logger.error("No hourly forecast periods could be processed.")
             raise HTTPException(status_code=500, detail="Could not process hourly forecast data.")


        # 5. Persist to Database
        logger.info("Persisting processed data to database.")
        await crud.create_current_weather_data(db, db_weather_request.request_id, processed_current_forecast)
        
        hourly_forecast_db_objects = [
            schemas.HourlyWeatherForecastCreate(
                request_id=db_weather_request.request_id,
                **hourly_data.model_dump() # Pydantic v2
            ) for hourly_data in processed_hourly_forecasts
        ]
        await crud.create_hourly_weather_forecasts(db, hourly_forecast_db_objects)

        # 6. Update weather request status to SUCCESS
        await crud.update_weather_request_status(db, db_weather_request.request_id, "SUCCESS")
        logger.info(f"Successfully processed weather request ID: {db_weather_request.request_id}")

        return schemas.WeatherRequestResponse(
            request_id=db_weather_request.request_id,
            user_id=db_weather_request.user_id,
            input_latitude=db_weather_request.input_latitude,
            input_longitude=db_weather_request.input_longitude,
            request_timestamp=db_weather_request.request_timestamp,
            status="SUCCESS",
            message="Weather data processed and stored successfully."
        )

    except HTTPException as http_exc:
        logger.error(f"HTTP Exception during processing request {db_weather_request.request_id}: {http_exc.detail}")
        await crud.update_weather_request_status(db, db_weather_request.request_id, "FAILED", str(http_exc.detail))
        raise http_exc
    except Exception as e:
        logger.exception(f"An unexpected error occurred while processing request ID {db_weather_request.request_id}: {str(e)}")
        await crud.update_weather_request_status(db, db_weather_request.request_id, "FAILED", f"An unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected server error occurred: {str(e)}")

@app.get("/weather/requests/{request_id}", response_model=schemas.WeatherRequestDetails)
async def get_weather_request_details(request_id: int, db: AsyncSession = Depends(get_db_session)):
    """
    Retrieves the details of a specific weather request, including its current and hourly forecasts.
    """
    logger.info(f"Fetching details for weather request ID: {request_id}")
    db_weather_request = await crud.get_weather_request(db, request_id)
    if db_weather_request is None:
        logger.warning(f"Weather request ID {request_id} not found.")
        raise HTTPException(status_code=404, detail="Weather request not found")

    current_data = await crud.get_current_weather_by_request_id(db, request_id)
    hourly_data_list = await crud.get_hourly_forecasts_by_request_id(db, request_id)
    
    logger.info(f"Successfully fetched details for weather request ID: {request_id}")
    return schemas.WeatherRequestDetails(
        request_info=schemas.WeatherRequest.model_validate(db_weather_request), #Pydantic v2
        current_weather=schemas.CurrentWeatherProcessed.model_validate(current_data) if current_data else None,
        hourly_forecasts=[schemas.HourlyWeatherProcessed.model_validate(h) for h in hourly_data_list]
    )

@app.get("/health", status_code=200)
async def health_check():
    return {"status": "healthy"}

# This is for running with `python main.py` for simple local testing if needed,
# but `uvicorn main:app --reload` is preferred for development.
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# --- app/schemas.py ---
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from datetime import datetime

# --- Request Schemas ---
class WeatherRequestCreate(BaseModel):
    user_id: str = Field(..., example="user_123")
    latitude: float = Field(..., ge=-90, le=90, example=40.7128)
    longitude: float = Field(..., ge=-180, le=180, example=-74.0060)

# --- NWS API Related Schemas (internal use primarily) ---
class NWSGridPointProperties(BaseModel):
    gridId: str
    gridX: int
    gridY: int
    forecast: HttpUrl
    forecastHourly: HttpUrl
    forecastZone: Optional[HttpUrl] = None # For potential future use with forecast area

class NWSGridInfo(BaseModel):
    grid_id: str
    grid_x: int
    grid_y: int
    forecast_url: HttpUrl
    forecast_hourly_url: HttpUrl

# --- Database Model Schemas (mirroring SQLAlchemy models, for API responses) ---
class WeatherRequestBase(BaseModel):
    user_id: str
    input_latitude: float
    input_longitude: float
    nws_grid_id: Optional[str] = None
    nws_grid_x: Optional[int] = None
    nws_grid_y: Optional[int] = None
    status: str = "PENDING"
    error_message: Optional[str] = None

class WeatherRequest(WeatherRequestBase):
    request_id: int
    request_timestamp: datetime

    class Config:
        from_attributes = True # Pydantic v2 for ORM mode

class CurrentWeatherBase(BaseModel):
    forecast_time: datetime
    short_forecast: Optional[str] = None
    temperature: Optional[float] = None
    temperature_unit: Optional[str] = Field(None, pattern="^[CF]$")
    wind_speed: Optional[str] = None # e.g., "10 mph", "10 to 15 mph"
    wind_direction: Optional[str] = None
    precipitation_probability: Optional[float] = Field(None, ge=0, le=1) # 0.0 to 1.0
    relative_humidity_percent: Optional[float] = Field(None, ge=0, le=100)
    dewpoint_celsius: Optional[float] = None

class CurrentWeatherProcessed(CurrentWeatherBase):
    # This schema is used when returning processed data
    # It can be identical to base or add fields if needed
    pass
    class Config:
        from_attributes = True

class CurrentWeatherDataCreate(CurrentWeatherBase):
    request_id: int

class CurrentWeatherDataDB(CurrentWeatherDataCreate):
    id: int
    processed_at: datetime
    class Config:
        from_attributes = True


class HourlyWeatherBase(BaseModel):
    forecast_time: datetime
    temperature: Optional[float] = None
    temperature_unit: Optional[str] = Field(None, pattern="^[CF]$")
    wind_speed_mph: Optional[float] = None
    wind_direction: Optional[str] = None
    precipitation_probability: float = Field(..., ge=0, le=1) # NWS provides this, so it's required
    short_forecast: Optional[str] = None
    # Enriched features
    temp_ratio_to_period_avg: Optional[float] = None
    wind_exceeds_daily_avg: Optional[bool] = None
    # latitude_distance_from_forecast_area: float = 0.0 # As per design doc, assumed 0
    # longitude_distance_from_forecast_area: float = 0.0 # As per design doc, assumed 0


class HourlyWeatherProcessed(HourlyWeatherBase):
    # This schema is used when returning processed data
    pass
    class Config:
        from_attributes = True

class HourlyWeatherForecastCreate(HourlyWeatherBase):
    request_id: int # This will be set when creating DB objects

class HourlyWeatherForecastDB(HourlyWeatherForecastCreate):
    processed_at: datetime
    class Config:
        from_attributes = True

# --- API Response Schemas ---
class WeatherRequestResponse(BaseModel):
    request_id: int
    user_id: str
    input_latitude: float
    input_longitude: float
    request_timestamp: datetime
    status: str
    message: str

class WeatherRequestDetails(BaseModel):
    request_info: WeatherRequest
    current_weather: Optional[CurrentWeatherProcessed] = None
    hourly_forecasts: List[HourlyWeatherProcessed] = []


# --- app/crud.py ---
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update
from . import models, schemas
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

async def create_weather_request(db: AsyncSession, request: schemas.WeatherRequestCreate) -> models.WeatherRequest:
    """
    Creates a new weather request record in the database.
    """
    db_request = models.WeatherRequest(
        user_id=request.user_id,
        input_latitude=request.latitude,
        input_longitude=request.longitude,
        status="PENDING"
    )
    db.add(db_request)
    await db.commit()
    await db.refresh(db_request)
    logger.info(f"Created weather request {db_request.request_id} for user {request.user_id}")
    return db_request

async def update_weather_request_grid_info(db: AsyncSession, request_id: int, grid_info: schemas.NWSGridInfo):
    """
    Updates an existing weather request with NWS grid information.
    """
    stmt = (
        update(models.WeatherRequest)
        .where(models.WeatherRequest.request_id == request_id)
        .values(
            nws_grid_id=grid_info.grid_id,
            nws_grid_x=grid_info.grid_x,
            nws_grid_y=grid_info.grid_y
        )
    )
    await db.execute(stmt)
    await db.commit()
    logger.info(f"Updated grid info for weather request {request_id}")

async def update_weather_request_status(db: AsyncSession, request_id: int, status: str, error_message: Optional[str] = None):
    """
    Updates the status and optionally an error message for a weather request.
    """
    stmt = (
        update(models.WeatherRequest)
        .where(models.WeatherRequest.request_id == request_id)
        .values(status=status, error_message=error_message)
    )
    await db.execute(stmt)
    await db.commit()
    logger.info(f"Updated status to {status} for weather request {request_id}")


async def get_weather_request(db: AsyncSession, request_id: int) -> Optional[models.WeatherRequest]:
    """
    Retrieves a weather request by its ID.
    """
    result = await db.execute(select(models.WeatherRequest).filter(models.WeatherRequest.request_id == request_id))
    return result.scalars().first()

async def create_current_weather_data(db: AsyncSession, request_id: int, current_weather: schemas.CurrentWeatherProcessed) -> models.CurrentWeatherData:
    """
    Creates a current weather data record linked to a weather request.
    """
    db_current_weather = models.CurrentWeatherData(
        request_id=request_id,
        **current_weather.model_dump() #Pydantic v2
    )
    db.add(db_current_weather)
    await db.commit()
    await db.refresh(db_current_weather)
    logger.info(f"Stored current weather for request {request_id}")
    return db_current_weather

async def get_current_weather_by_request_id(db: AsyncSession, request_id: int) -> Optional[models.CurrentWeatherData]:
    """
    Retrieves current weather data for a given request_id.
    """
    result = await db.execute(
        select(models.CurrentWeatherData).filter(models.CurrentWeatherData.request_id == request_id)
    )
    return result.scalars().first()


async def create_hourly_weather_forecasts(db: AsyncSession, hourly_forecasts_data: List[schemas.HourlyWeatherForecastCreate]):
    """
    Creates multiple hourly weather forecast records.
    This is a bulk insert if the DB driver supports it efficiently,
    otherwise, it might loop. SQLAlchemy handles this.
    """
    db_objects = [models.HourlyWeatherForecast(**data.model_dump()) for data in hourly_forecasts_data] #Pydantic v2
    db.add_all(db_objects)
    await db.commit()
    # Note: Refreshing multiple objects from add_all might need individual handling if IDs are needed immediately.
    # For this use case, we typically don't need to refresh them right after bulk insert.
    logger.info(f"Stored {len(db_objects)} hourly forecasts for request {hourly_forecasts_data[0].request_id if hourly_forecasts_data else 'N/A'}")


async def get_hourly_forecasts_by_request_id(db: AsyncSession, request_id: int) -> List[models.HourlyWeatherForecast]:
    """
    Retrieves all hourly forecasts for a given request_id, ordered by forecast time.
    """
    result = await db.execute(
        select(models.HourlyWeatherForecast)
        .filter(models.HourlyWeatherForecast.request_id == request_id)
        .order_by(models.HourlyWeatherForecast.forecast_time)
    )
    return result.scalars().all()


# --- app/nws_service.py ---
import httpx
from fastapi import HTTPException
from .core.config import settings
from .schemas import NWSGridPointProperties, NWSGridInfo
import logging

logger = logging.getLogger(__name__)

# NWS API requires a User-Agent header.
# Format: (Application Name, Contact Information like email or website)
# Example: "MyWeatherApp/1.0 (myemail@example.com)"
# This should be set via an environment variable (NWS_USER_AGENT)
# Fallback is provided in settings, but it's better to set it explicitly.
NWS_API_HEADERS = {
    "User-Agent": settings.NWS_USER_AGENT,
    "Accept": "application/geo+json" # For /points endpoint, can also be application/ld+json
}
NWS_API_FORECAST_HEADERS = {
    "User-Agent": settings.NWS_USER_AGENT,
    "Accept": "application/json" # Forecasts are usually standard JSON
}


async def get_nws_gridpoint_info(latitude: float, longitude: float) -> NWSGridInfo:
    """
    Fetches NWS gridpoint information (grid ID, X, Y, and forecast URLs)
    for the given latitude and longitude.
    """
    points_url = f"{settings.NWS_API_BASE_URL}/points/{latitude:.4f},{longitude:.4f}"
    logger.info(f"Requesting NWS gridpoint info from: {points_url}")
    
    async with httpx.AsyncClient(timeout=settings.NWS_API_TIMEOUT) as client:
        try:
            response = await client.get(points_url, headers=NWS_API_HEADERS)
            response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
            data = response.json()
            
            properties = NWSGridPointProperties(**data.get("properties", {}))
            
            grid_info = NWSGridInfo(
                grid_id=properties.gridId,
                grid_x=properties.gridX,
                grid_y=properties.gridY,
                forecast_url=properties.forecast,
                forecast_hourly_url=properties.forecastHourly
            )
            logger.info(f"Successfully fetched NWS gridpoint info: {grid_info}")
            return grid_info
            
        except httpx.HTTPStatusError as e:
            logger.error(f"NWS API HTTPStatusError for gridpoint: {e.response.status_code} - {e.response.text}")
            detail = f"Error from NWS API ({e.response.status_code}): Could not retrieve gridpoint data. {e.response.json().get('detail', '')}"
            if e.response.status_code == 404:
                 detail = f"NWS API: No data found for coordinates {latitude},{longitude}. The location might be outside the NWS coverage area (e.g. over oceans or outside the US)."
            raise HTTPException(status_code=e.response.status_code, detail=detail)
        except httpx.RequestError as e:
            logger.error(f"NWS API RequestError for gridpoint: {str(e)}")
            raise HTTPException(status_code=503, detail=f"NWS API service unavailable or request error: {str(e)}")
        except Exception as e: # Catch Pydantic validation errors or other unexpected issues
            logger.exception(f"Unexpected error parsing NWS gridpoint response: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing NWS gridpoint response: {str(e)}")


async def get_nws_forecast(forecast_url: str) -> dict:
    """
    Fetches the general forecast from the provided NWS forecast URL.
    """
    logger.info(f"Requesting NWS general forecast from: {forecast_url}")
    async with httpx.AsyncClient(timeout=settings.NWS_API_TIMEOUT) as client:
        try:
            response = await client.get(forecast_url, headers=NWS_API_FORECAST_HEADERS)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched NWS general forecast from {forecast_url}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"NWS API HTTPStatusError for forecast: {e.response.status_code} - {e.response.text}")
            detail = f"Error from NWS API ({e.response.status_code}) fetching general forecast: {e.response.json().get('detail', '')}"
            raise HTTPException(status_code=e.response.status_code, detail=detail)
        except httpx.RequestError as e:
            logger.error(f"NWS API RequestError for forecast: {str(e)}")
            raise HTTPException(status_code=503, detail=f"NWS API service unavailable or request error for general forecast: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error parsing NWS forecast response: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing NWS forecast response: {str(e)}")


async def get_nws_forecast_hourly(hourly_forecast_url: str) -> dict:
    """
    Fetches the hourly forecast from the provided NWS hourly forecast URL.
    """
    logger.info(f"Requesting NWS hourly forecast from: {hourly_forecast_url}")
    async with httpx.AsyncClient(timeout=settings.NWS_API_TIMEOUT) as client:
        try:
            response = await client.get(hourly_forecast_url, headers=NWS_API_FORECAST_HEADERS)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched NWS hourly forecast from {hourly_forecast_url}")
            return data
        except httpx.HTTPStatusError as e:
            logger.error(f"NWS API HTTPStatusError for hourly forecast: {e.response.status_code} - {e.response.text}")
            detail = f"Error from NWS API ({e.response.status_code}) fetching hourly forecast: {e.response.json().get('detail', '')}"
            raise HTTPException(status_code=e.response.status_code, detail=detail)
        except httpx.RequestError as e:
            logger.error(f"NWS API RequestError for hourly forecast: {str(e)}")
            raise HTTPException(status_code=503, detail=f"NWS API service unavailable or request error for hourly forecast: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error parsing NWS hourly forecast response: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing NWS hourly forecast response: {str(e)}")


# --- app/processing_service.py ---
from typing import List, Dict, Any, Optional
from .schemas import CurrentWeatherProcessed, HourlyWeatherProcessed
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

def parse_temperature(temp_str: Any, unit_str: Any) -> Optional[float]:
    """Safely parses temperature, handling None or non-numeric values."""
    if temp_str is None:
        return None
    try:
        return float(temp_str)
    except (ValueError, TypeError):
        logger.warning(f"Could not parse temperature: {temp_str}")
        return None

def parse_wind_speed(wind_speed_str: Optional[str]) -> Optional[float]:
    """
    Parses wind speed string like "10 mph" or "10 to 15 mph" to get an average or single value in mph.
    Returns None if parsing fails.
    """
    if not wind_speed_str:
        return None
    
    wind_speed_str = wind_speed_str.lower()
    
    # Try to extract numbers
    numbers = re.findall(r'\d+\.?\d*', wind_speed_str)
    
    if not numbers:
        return None
        
    try:
        numeric_values = [float(n) for n in numbers]
        if "mph" not in wind_speed_str and "kt" in wind_speed_str: # Convert knots to mph if specified
            # 1 kt = 1.15078 mph
            numeric_values = [n * 1.15078 for n in numeric_values]
        
        if len(numeric_values) == 1:
            return numeric_values[0]
        elif len(numeric_values) > 1: # e.g. "10 to 15 mph" -> average
            return sum(numeric_values) / len(numeric_values)
    except ValueError:
        logger.warning(f"Could not parse wind speed numbers from: {wind_speed_str}")
        return None
    return None

def process_current_forecast(current_period: Dict[str, Any]) -> CurrentWeatherProcessed:
    """
    Processes a single period from the NWS general forecast (assumed to be the current one).
    """
    if not current_period:
        logger.warning("Received empty current_period for processing.")
        # Return a default or mostly empty object to avoid downstream errors
        return CurrentWeatherProcessed(
            forecast_time=datetime.utcnow(), # Fallback, ideally should come from data
            precipitation_probability=0.0 # Default if not available
        )

    logger.debug(f"Processing current forecast period: {current_period}")
    
    # NWS API sometimes returns probabilityOfPrecipitation as {"unitCode": "wmoUnit:percent", "value": null}
    # or sometimes it's missing. Handle this gracefully.
    precip_prop = current_period.get("probabilityOfPrecipitation", {})
    precip_value = precip_prop.get("value") if precip_prop else None
    if precip_value is not None:
        precipitation_probability = float(precip_value) / 100.0
    else:
        precipitation_probability = 0.0 # Default if not available or null

    dewpoint_prop = current_period.get("dewpoint", {})
    dewpoint_value = dewpoint_prop.get("value") if dewpoint_prop else None

    humidity_prop = current_period.get("relativeHumidity", {})
    humidity_value = humidity_prop.get("value") if humidity_prop else None
    
    return CurrentWeatherProcessed(
        forecast_time=datetime.fromisoformat(current_period.get("startTime", datetime.utcnow().isoformat())),
        short_forecast=current_period.get("shortForecast"),
        temperature=parse_temperature(current_period.get("temperature"), current_period.get("temperatureUnit")),
        temperature_unit=current_period.get("temperatureUnit"),
        wind_speed=current_period.get("windSpeed"), # Keep original string for now
        wind_direction=current_period.get("windDirection"),
        precipitation_probability=precipitation_probability,
        relative_humidity_percent=float(humidity_value) if humidity_value is not None else None,
        dewpoint_celsius=float(dewpoint_value) if dewpoint_value is not None else None,
    )

def process_and_enrich_hourly_forecasts(
    hourly_periods: List[Dict[str, Any]],
    input_latitude: float, # For potential future use with distance calc
    input_longitude: float # For potential future use with distance calc
) -> List[HourlyWeatherProcessed]:
    """
    Processes raw hourly forecast periods from NWS and enriches them.
    Takes the next 24 relevant periods.
    """
    if not hourly_periods:
        logger.warning("Received empty hourly_periods list for processing.")
        return []

    processed_forecasts: List[HourlyWeatherProcessed] = []
    
    # Filter to the next 24 available periods from now or first valid period
    # NWS hourly can sometimes start in the past, find first future/current point
    now = datetime.utcnow()
    relevant_periods = []
    start_index = 0
    for i, period in enumerate(hourly_periods):
        try:
            period_start_time = datetime.fromisoformat(period.get("startTime").replace("Z", "+00:00")) # Ensure timezone aware
            if period_start_time >= now.replace(tzinfo=period_start_time.tzinfo): # Compare timezone-aware
                start_index = i
                break
        except Exception: # If startTime is missing or invalid
            continue 
    
    # Take up to 24 periods from the determined start_index
    hourly_periods_to_process = hourly_periods[start_index : start_index + 24]
    
    if not hourly_periods_to_process:
        logger.warning("No future hourly forecast periods found to process.")
        return []

    # --- Calculate daily averages for enrichment ---
    total_temp = 0
    total_wind_speed_mph = 0
    valid_temp_count = 0
    valid_wind_count = 0

    temp_hourly_values = [] # Store temperatures for ratio calculation

    for period in hourly_periods_to_process:
        temp = parse_temperature(period.get("temperature"), period.get("temperatureUnit"))
        if temp is not None and period.get("temperatureUnit") == "F": # Assuming F, convert to C if needed or standardize
            total_temp += temp
            valid_temp_count += 1
            temp_hourly_values.append(temp)
        elif temp is not None and period.get("temperatureUnit") == "C": # Handle Celsius if present
            # For simplicity, let's assume all temps are consistent or NWS provides one unit.
            # If mixed, conversion would be needed. Here, just add if valid.
            total_temp += temp # This assumes a consistent unit for averaging.
            valid_temp_count += 1
            temp_hourly_values.append(temp)


        wind_speed_mph = parse_wind_speed(period.get("windSpeed"))
        if wind_speed_mph is not None:
            total_wind_speed_mph += wind_speed_mph
            valid_wind_count += 1
    
    avg_temp_24h = (total_temp / valid_temp_count) if valid_temp_count > 0 else None
    avg_wind_speed_24h_mph = (total_wind_speed_mph / valid_wind_count) if valid_wind_count > 0 else None

    logger.info(f"Calculated 24h averages: Temp={avg_temp_24h}, WindSpeedMPH={avg_wind_speed_24h_mph}")

    # --- Process each hourly period ---
    for period in hourly_periods_to_process:
        temp = parse_temperature(period.get("temperature"), period.get("temperatureUnit"))
        wind_speed_mph = parse_wind_speed(period.get("windSpeed"))

        temp_ratio = None
        if temp is not None and avg_temp_24h is not None and avg_temp_24h != 0:
            temp_ratio = temp / avg_temp_24h
        
        wind_exceeds_avg = None
        if wind_speed_mph is not None and avg_wind_speed_24h_mph is not None:
            wind_exceeds_avg = wind_speed_mph > avg_wind_speed_24h_mph

        precip_prop = period.get("probabilityOfPrecipitation", {})
        precip_value = precip_prop.get("value") if precip_prop else None
        if precip_value is not None:
            precipitation_probability = float(precip_value) / 100.0
        else: # If NWS hourly doesn't have it, which is rare, but good to default
            precipitation_probability = 0.0 

        processed_forecasts.append(
            HourlyWeatherProcessed(
                forecast_time=datetime.fromisoformat(period.get("startTime")),
                temperature=temp,
                temperature_unit=period.get("temperatureUnit"),
                wind_speed_mph=wind_speed_mph,
                wind_direction=period.get("windDirection"),
                precipitation_probability=precipitation_probability,
                short_forecast=period.get("shortForecast"),
                temp_ratio_to_period_avg=temp_ratio,
                wind_exceeds_daily_avg=wind_exceeds_avg,
                # latitude_distance_from_forecast_area: As per design, this is 0
                # longitude_distance_from_forecast_area: As per design, this is 0
            )
        )
    logger.info(f"Processed {len(processed_forecasts)} hourly forecast periods.")
    return processed_forecasts


# --- app/models.py ---
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # for server_default=func.now()
from .database import Base # Import Base from your database setup
from datetime import datetime

class WeatherRequest(Base):
    __tablename__ = "weather_requests"

    request_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(String(255), nullable=False, index=True)
    input_latitude = Column(Float, nullable=False)
    input_longitude = Column(Float, nullable=False)
    
    nws_grid_id = Column(String(10), nullable=True) # e.g., "OKX"
    nws_grid_x = Column(Integer, nullable=True)
    nws_grid_y = Column(Integer, nullable=True)
    
    request_timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    status = Column(String(50), default="PENDING", nullable=False) # PENDING, SUCCESS, FAILED
    error_message = Column(Text, nullable=True)

    # Relationships
    current_weather = relationship("CurrentWeatherData", back_populates="request", uselist=False, cascade="all, delete-orphan")
    hourly_forecasts = relationship("HourlyWeatherForecast", back_populates="request", cascade="all, delete-orphan")


class CurrentWeatherData(Base):
    __tablename__ = "current_weather_data"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("weather_requests.request_id", ondelete="CASCADE"), nullable=False, index=True)
    
    forecast_time = Column(DateTime(timezone=True), nullable=False)
    short_forecast = Column(String(255), nullable=True)
    temperature = Column(Float, nullable=True)
    temperature_unit = Column(String(1), nullable=True) # 'F' or 'C'
    wind_speed = Column(String(50), nullable=True) # e.g., "10 mph"
    wind_direction = Column(String(10), nullable=True)
    precipitation_probability = Column(Float, nullable=True) # Percentage, e.g., 0.30 for 30%
    relative_humidity_percent = Column(Float, nullable=True)
    dewpoint_celsius = Column(Float, nullable=True)
    
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    request = relationship("WeatherRequest", back_populates="current_weather")


class HourlyWeatherForecast(Base):
    __tablename__ = "hourly_weather_forecasts" # This will be a TimescaleDB hypertable

    # TimescaleDB typically uses a composite primary key for hypertables if you don't specify one.
    # For simplicity with SQLAlchemy, we can define one, or let Timescale handle it.
    # If using request_id and forecast_time as PK, ensure they are part of the hypertable creation.
    # id = Column(Integer, primary_key=True, autoincrement=True) # Optional, if you prefer a surrogate key
    request_id = Column(Integer, ForeignKey("weather_requests.request_id", ondelete="CASCADE"), primary_key=True, nullable=False)
    forecast_time = Column(DateTime(timezone=True), primary_key=True, nullable=False) # This will be the time dimension for TimescaleDB

    temperature = Column(Float, nullable=True)
    temperature_unit = Column(String(1), nullable=True)
    wind_speed_mph = Column(Float, nullable=True)
    wind_direction = Column(String(10), nullable=True)
    precipitation_probability = Column(Float, nullable=False) # e.g., 0.30 for 30%
    short_forecast = Column(String(255), nullable=True)
    
    # Enriched features
    temp_ratio_to_period_avg = Column(Float, nullable=True)
    wind_exceeds_daily_avg = Column(Boolean, nullable=True)
    # latitude_distance_from_forecast_area = Column(Float, default=0.0) # As per design
    # longitude_distance_from_forecast_area = Column(Float, default=0.0) # As per design
    
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    request = relationship("WeatherRequest", back_populates="hourly_forecasts")

    # For TimescaleDB, you'd execute:
    # SELECT create_hypertable('hourly_weather_forecasts', 'forecast_time');
    # This is done outside of SQLAlchemy models, typically via Alembic migration or manually.


# --- app/database.py ---
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from .core.config import settings
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = settings.DATABASE_URL

# Create an async engine instance.
# echo=True will log all SQL statements, useful for debugging.
engine = create_async_engine(DATABASE_URL, echo=settings.DB_ECHO_LOG)

# Create a configured "AsyncSession" class.
# expire_on_commit=False prevents attributes from being expired after commit.
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False, # Recommended for async
    autocommit=False # Recommended for async
)

# Base class for declarative class definitions.
Base = declarative_base()

async def initialize_database():
    """
    Initializes the database by creating all tables defined in the Base metadata.
    This should ideally be handled by Alembic migrations in a production setup.
    For TimescaleDB hypertables, the create_hypertable command needs separate execution.
    """
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all) # Use with caution, drops all tables
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created (if they didn't exist).")
        
        # After tables are created, convert hourly_weather_forecasts to a hypertable.
        # This is idempotent; it won't fail if the table is already a hypertable.
        # Note: In a production system, this should be part of your migrations (e.g., Alembic).
        try:
            await conn.execute(
                "SELECT create_hypertable('hourly_weather_forecasts', 'forecast_time', if_not_exists => TRUE);"
            )
            logger.info("'hourly_weather_forecasts' table converted to or confirmed as hypertable.")
        except Exception as e:
            # This might fail if the extension isn't enabled or table doesn't exist yet,
            # or if it's already a hypertable and if_not_exists is not supported/used correctly.
            # TimescaleDB versions might vary in exact syntax for 'if_not_exists' within execute.
            # Check TimescaleDB logs if issues.
            logger.warning(f"Could not convert 'hourly_weather_forecasts' to hypertable or already a hypertable: {e}")


async def get_db_session() -> AsyncSession:
    """
    Dependency that provides a database session for each request.
    Ensures the session is closed after the request is finished.
    """
    async_session = AsyncSessionLocal()
    try:
        yield async_session
        await async_session.commit() # Commit any changes made during the request
    except Exception:
        await async_session.rollback() # Rollback in case of an error
        raise
    finally:
        await async_session.close()

async def close_db_pool():
    """
    Closes the database connection pool.
    Called during application shutdown.
    """
    if engine:
        await engine.dispose()
        logger.info("Database engine disposed.")


# --- app/core/config.py ---
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Application settings
    APP_NAME: str = "Weather Enrichment Microservice"
    
    # NWS API settings
    NWS_API_BASE_URL: str = "https://api.weather.gov"
    NWS_USER_AGENT: str = "MyWeatherApp/1.0 (change_me@example.com)" # IMPORTANT: Change this!
    NWS_API_TIMEOUT: int = 20 # Timeout for NWS API requests in seconds

    # Database settings
    # Example: postgresql+asyncpg://weather_user:weather_password@db:5432/weather_db
    DATABASE_URL: str = "postgresql+asyncpg://weather_user:weather_password@localhost:5433/weather_db"
    DB_ECHO_LOG: bool = False # Set to True to see SQL queries

    class Config:
        env_file = ".env" # Load .env file if it exists
        env_file_encoding = 'utf-8'
        extra = 'ignore' # Ignore extra fields from .env

@lru_cache() # Cache the settings object
def get_settings() -> Settings:
    return Settings()

settings = get_settings()

# --- requirements.txt ---
# FastAPI and Uvicorn
fastapi
uvicorn[standard]

# SQLAlchemy and Async PostgreSQL Driver
sqlalchemy>=1.4.0 # For async support
asyncpg # PostgreSQL async driver
psycopg2-binary # Often needed by Alembic or other tools, even with asyncpg

# HTTP Client for NWS API
httpx

# Pydantic and Pydantic-Settings
pydantic
pydantic-settings

# For database migrations (optional but recommended for production)
# alembic

# For logging (uvicorn uses logging by default)

# --- .env.example ---
# Copy this file to .env and fill in your actual values.

# Application Configuration
# NWS_USER_AGENT: Your application name and contact email for NWS API. This is REQUIRED by NWS.
# Example: NWS_USER_AGENT="MyCoolWeatherApp/0.1 (developer@example.com)"
NWS_USER_AGENT="MyWeatherApp/1.0 (default_contact@example.com)"

# Database Configuration for TimescaleDB/PostgreSQL
# Format: postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DB_NAME
DATABASE_URL="postgresql+asyncpg://weather_user:weather_password@db:5432/weather_db"

# Set to true to see all SQL queries executed by SQLAlchemy (can be verbose)
DB_ECHO_LOG=False

# NWS API Timeout in seconds
NWS_API_TIMEOUT=20

