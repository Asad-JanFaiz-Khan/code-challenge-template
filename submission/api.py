"""
Flask REST API for weather and yearly stats (Problem 4).

Usage:
    export DATABASE_URL=sqlite:///weather.db
    python -m submission.app

Or embed with `create_app(database_url=...)` for testing.
"""

from flask import Flask, request, jsonify, current_app
from datetime import datetime
from pathlib import Path
import os

from database import get_database_manager
from models import WeatherRecord, WeatherStation, YearlyStationStats


def create_app(database_url: str | None = None) -> Flask:
    app = Flask(__name__)

    db_url = database_url or os.environ.get('DATABASE_URL') or 'sqlite:///weather.db'
    app.config['DATABASE_URL'] = db_url
    app.config['DB_MANAGER'] = get_database_manager(db_url)


    @app.route('/api/weather', methods=['GET'])
    def get_weather():
        dbm = current_app.config['DB_MANAGER']
        session = dbm.get_session()
        """GET /api/weather

        Returns paginated weather records. Supports filtering by station and date range.

        Query parameters:
        - station_id: station code (string)
        - date: YYYY-MM-DD exact date
        - start_date / end_date: YYYY-MM-DD range
        - limit / offset: pagination
        """
        try:
            station_param = request.args.get('station_id', type=str)
            date_str = request.args.get('date', type=str)
            start_date_str = request.args.get('start_date', type=str)
            end_date_str = request.args.get('end_date', type=str)
            limit = request.args.get('limit', default=100, type=int)
            offset = request.args.get('offset', default=0, type=int)

            limit = min(max(1, limit), 10000)

            query = session.query(WeatherRecord).join(WeatherStation)

            if station_param:
                # station_param might be station_id string (e.g., 'USC00110072')
                query = query.filter(WeatherStation.station_id == station_param)

            if date_str:
                try:
                    d = datetime.strptime(date_str, '%Y-%m-%d').date()
                    query = query.filter(WeatherRecord.observation_date == d)
                except ValueError:
                    return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

            if start_date_str:
                try:
                    s = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                    query = query.filter(WeatherRecord.observation_date >= s)
                except ValueError:
                    return jsonify({'error': 'Invalid start_date format. Use YYYY-MM-DD'}), 400

            if end_date_str:
                try:
                    e = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                    query = query.filter(WeatherRecord.observation_date <= e)
                except ValueError:
                    return jsonify({'error': 'Invalid end_date format. Use YYYY-MM-DD'}), 400

            total = query.count()
            rows = query.order_by(WeatherRecord.observation_date).offset(offset).limit(limit).all()

            data = []
            for r in rows:
                data.append({
                    'id': r.id,
                    'station_id': r.station.station_id,
                    'date': r.observation_date.isoformat(),
                    'max_temperature_celsius': r.max_temperature_celsius,
                    'min_temperature_celsius': r.min_temperature_celsius,
                    'precipitation_mm': r.precipitation_mm,
                })

            return jsonify({'data': data, 'pagination': {'total_count': total, 'limit': limit, 'offset': offset, 'returned': len(data)}})
        finally:
            session.close()


    @app.route('/api/weather/stats', methods=['GET'])
    def get_weather_stats():
        dbm = current_app.config['DB_MANAGER']
        session = dbm.get_session()
        """GET /api/weather/stats

        Returns paginated yearly per-station statistics. Supports filtering by station and year range.

        Query parameters:
        - station_id: station code (string)
        - year / start_year / end_year: integer year filters
        - limit / offset: pagination
        """
        try:
            station_param = request.args.get('station_id', type=str)
            year = request.args.get('year', type=int)
            start_year = request.args.get('start_year', type=int)
            end_year = request.args.get('end_year', type=int)
            limit = request.args.get('limit', default=100, type=int)
            offset = request.args.get('offset', default=0, type=int)

            limit = min(max(1, limit), 10000)

            query = session.query(YearlyStationStats).join(WeatherStation)

            if station_param:
                query = query.filter(WeatherStation.station_id == station_param)
            if year:
                query = query.filter(YearlyStationStats.year == year)
            if start_year:
                query = query.filter(YearlyStationStats.year >= start_year)
            if end_year:
                query = query.filter(YearlyStationStats.year <= end_year)

            total = query.count()
            rows = query.order_by(YearlyStationStats.station_id, YearlyStationStats.year).offset(offset).limit(limit).all()

            data = []
            for r in rows:
                # translate station PK to station_id string
                station_row = session.query(WeatherStation).filter_by(id=r.station_id).first()
                station_code = station_row.station_id if station_row else None
                data.append({
                    'station_id': station_code,
                    'year': int(r.year),
                    'avg_max_celsius': r.avg_max_celsius,
                    'avg_min_celsius': r.avg_min_celsius,
                    'total_precip_cm': r.total_precip_cm,
                })

            return jsonify({'data': data, 'pagination': {'total_count': total, 'limit': limit, 'offset': offset, 'returned': len(data)}})
        finally:
            session.close()


    @app.route('/openapi.json')
    def openapi_json():
        # Provide a more detailed OpenAPI spec so Swagger UI shows parameters and response shapes.
        spec = {
            'openapi': '3.0.0',
            'info': {'title': 'Weather API', 'version': '1.0', 'description': 'Weather and crop-yield API'},
            'paths': {
                '/api/weather': {
                    'get': {
                        'summary': 'List weather records',
                        'parameters': [
                            {'name': 'station_id', 'in': 'query', 'schema': {'type': 'string'}, 'description': 'Station code'},
                            {'name': 'date', 'in': 'query', 'schema': {'type': 'string', 'format': 'date'}, 'description': 'Exact date YYYY-MM-DD'},
                            {'name': 'start_date', 'in': 'query', 'schema': {'type': 'string', 'format': 'date'}},
                            {'name': 'end_date', 'in': 'query', 'schema': {'type': 'string', 'format': 'date'}},
                            {'name': 'limit', 'in': 'query', 'schema': {'type': 'integer'}},
                            {'name': 'offset', 'in': 'query', 'schema': {'type': 'integer'}},
                        ],
                        'responses': {
                            '200': {
                                'description': 'A list of weather records',
                                'content': {
                                    'application/json': {
                                        'schema': {
                                            'type': 'object',
                                            'properties': {
                                                'data': {
                                                    'type': 'array',
                                                    'items': {
                                                        'type': 'object',
                                                        'properties': {
                                                            'id': {'type': 'integer'},
                                                            'station_id': {'type': 'string'},
                                                            'date': {'type': 'string', 'format': 'date'},
                                                            'max_temperature_celsius': {'type': ['number', 'null']},
                                                            'min_temperature_celsius': {'type': ['number', 'null']},
                                                            'precipitation_mm': {'type': ['number', 'null']},
                                                        }
                                                    }
                                                },
                                                'pagination': {'type': 'object'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                '/api/weather/stats': {
                    'get': {
                        'summary': 'Yearly per-station statistics',
                        'parameters': [
                            {'name': 'station_id', 'in': 'query', 'schema': {'type': 'string'}},
                            {'name': 'year', 'in': 'query', 'schema': {'type': 'integer'}},
                            {'name': 'start_year', 'in': 'query', 'schema': {'type': 'integer'}},
                            {'name': 'end_year', 'in': 'query', 'schema': {'type': 'integer'}},
                            {'name': 'limit', 'in': 'query', 'schema': {'type': 'integer'}},
                            {'name': 'offset', 'in': 'query', 'schema': {'type': 'integer'}},
                        ],
                        'responses': {
                            '200': {
                                'description': 'A list of yearly station statistics',
                                'content': {
                                    'application/json': {
                                        'schema': {
                                            'type': 'object',
                                            'properties': {
                                                'data': {
                                                    'type': 'array',
                                                    'items': {
                                                        'type': 'object',
                                                        'properties': {
                                                            'station_id': {'type': 'string'},
                                                            'year': {'type': 'integer'},
                                                            'avg_max_celsius': {'type': ['number', 'null']},
                                                            'avg_min_celsius': {'type': ['number', 'null']},
                                                            'total_precip_cm': {'type': ['number', 'null']},
                                                        }
                                                    }
                                                },
                                                'pagination': {'type': 'object'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return jsonify(spec)


    @app.route('/docs')
    def swagger_ui():
        html = '''<!doctype html>
<html>
  <head>
    <title>Swagger UI</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@4/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@4/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui' });
    </script>
  </body>
</html>'''
        return html

    return app


app = create_app()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
