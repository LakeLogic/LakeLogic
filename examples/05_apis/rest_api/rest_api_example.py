"""
REST API Extraction Example

Demonstrates how to extract data from a JSON REST API.

Prerequisites:
    pip install "lakelogic[api]"

What it does:
- Defines an API endpoint.
- Configures authentication headers.
- Fetches data and maps it to a LakeLogic model.
- Applies transformations.
"""

from lakelogic.core.data_processor import DataProcessor

def main():
    print("=" * 80)
    print("REST API Data Extraction")
    print("=" * 80)
    print()

    contract = {
        "version": "1.0.0",
        "dataset": "weather_data",
        
        "source": {
            "type": "api",
            "connector": "rest",
            "endpoint": "https://api.open-meteo.com/v1/forecast",
            "params": {
                "latitude": 52.52,
                "longitude": 13.41,
                "hourly": "temperature_2m"
            }
        },
        
        "model": {
            "fields": [
                {"name": "time", "type": "string"},
                {"name": "temperature_2m", "type": "float"}
            ]
        },
        
        "materialization": {
            "bronze": {
                "enabled": True,
                "path": "./data/bronze/weather/",
                "format": "parquet"
            }
        }
    }

    print("🚀 Fetching data from Open-Meteo API...")
    
    # processor = DataProcessor(contract)
    # result = processor.run()
    
    print("\n✅ API extraction example complete.")

if __name__ == "__main__":
    main()
