# Universal Order Validator

A configuration-driven order validation system for telecom operations.

## Features

- ✅ Config-driven validation rules (no code changes needed)
- ✅ Support multiple order types (Change Plan, Change Device, Disconnect, etc.)
- ✅ Real-time MSISDN validation
- ✅ Asset hierarchy validation
- ✅ REST API endpoints
- ✅ CLI support

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Salesforce Connection

Create `.env` file:

```env
SF_USERNAME=your_username
SF_PASSWORD=your_password
SF_SECURITY_TOKEN=your_token
SF_DOMAIN=login
```

### 3. Run API Server

```bash
python main.py --mode api
```

Server runs at `http://localhost:5000`

### 4. Validate MSISDN (CLI)

```bash
python main.py --mode cli --msisdn 12218071145
```

## API Endpoints

### Validate Yesterday's Orders
```
GET /api/validate/yesterday
```

### Validate Specific MSISDN
```
GET /api/validate/{msisdn}
POST /api/validate
Content-Type: application/json

{
  "msisdn": ""
}
```

### Health Check
```
GET /health
```

## Configuration

Edit `config/config.json` to:
- Add/modify order types
- Update validation rules
- Change SOQL queries
- Add new checks

No code changes required!

## Project Structure

```
order_validation/
├── config/                 # Configuration files
│   ├── config.json        # All validation rules
│   └── config_loader.py   # Config loading
├── modules/               # Business logic
│   ├── order_fetcher.py   # Fetch orders
│   ├── order_filter.py    # Filter & group orders
│   ├── asset_fetcher.py   # Fetch assets
│   ├── validation_engine.py # Run validations
│   └── response_builder.py  # Build responses
├── models/                # Data models
│   └── data_models.py     # Pydantic models
├── api/                   # API layer
│   └── endpoints.py       # Flask endpoints
├── utils/                 # Utilities
│   └── logger_config.py   # Logging setup
├── main.py                # Application entry point
└── requirements.txt       # Dependencies
```

## Response Format

### Success Response

```json
{
  "status": "success",
  "date_validated": "2025-11-10",
  "validated_msisdns": [
    {
      "msisdn": "12218071145",
      "latest_order": {...},
      "assets": {...},
      "validations": {...},
      "summary": {...}
    }
  ]
}
```

## License

Proprietary
