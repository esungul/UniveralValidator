import logging
from flask import Blueprint, request, jsonify
from typing import Dict, Any

logger = logging.getLogger(__name__)

validator_bp = Blueprint('validator', __name__, url_prefix='/api')

# These will be injected by main.py
validator_service = None

def set_validator_service(service):
    """Dependency injection for validator service"""
    global validator_service
    validator_service = service

@validator_bp.route('/validate/yesterday', methods=['GET'])
def validate_yesterday():
    """Validate all orders from yesterday"""
    try:
        logger.info("[API] GET /api/validate/yesterday")
        response = validator_service.validate_orders_yesterday()
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"[API] Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@validator_bp.route('/validate', methods=['POST'])
def validate_order():
    """Validate a specific MSISDN"""
    try:
        data = request.get_json()
        msisdn = data.get("msisdn")
        
        if not msisdn:
            return jsonify({"status": "error", "message": "MSISDN is required"}), 400
        
        logger.info(f"[API] POST /api/validate MSISDN: {msisdn}")
        response = validator_service.validate_msisdn(msisdn)
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"[API] Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@validator_bp.route('/validate/<msisdn>', methods=['GET'])
def validate_by_path(msisdn):
    """Validate a specific MSISDN (path parameter)"""
    try:
        logger.info(f"[API] GET /api/validate/{msisdn}")
        response = validator_service.validate_msisdn(msisdn)
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"[API] Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
