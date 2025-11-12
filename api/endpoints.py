import logging
from flask import Blueprint, request, jsonify, Response
from typing import Dict, Any
import csv
import io
import datetime

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

# ===== NEW BULK VALIDATION ENDPOINTS =====

@validator_bp.route('/validate/bulk/yesterday', methods=['POST'])
def validate_bulk_yesterday():
    """Bulk validate all yesterday's orders"""
    try:
        logger.info("[API] POST /api/validate/bulk/yesterday")
        
        # Check if bulk validator is available
        if not hasattr(validator_service, 'validate_bulk_yesterday'):
            return jsonify({
                "status": "error", 
                "message": "Bulk validation not available. Please check implementation."
            }), 501
        
        response = validator_service.validate_bulk_yesterday()
        
        # Handle CSV output
        if isinstance(response, dict) and response.get("format") == "csv":
            csv_string = response.get("csv_string", "")
            filename = f"bulk_validation_{response.get('summary', {}).get('timestamp', 'results')}.csv"
            
            return Response(
                csv_string,
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"[API] Bulk validation error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@validator_bp.route('/validate/bulk/msisdns', methods=['POST'])
def validate_bulk_msisdns():
    """Bulk validate specific MSISDNs provided in request"""
    try:
        data = request.get_json()
        msisdns = data.get("msisdns", [])
        
        if not msisdns or not isinstance(msisdns, list):
            return jsonify({
                "status": "error", 
                "message": "MSISDNs list is required and must be an array"
            }), 400
        
        logger.info(f"[API] POST /api/validate/bulk/msisdns - {len(msisdns)} MSISDNs")
        
        # Check if bulk MSISDN validator is available
        if not hasattr(validator_service, 'validate_bulk_msisdns'):
            return jsonify({
                "status": "error", 
                "message": "Bulk MSISDN validation not available"
            }), 501
        
        response = validator_service.validate_bulk_msisdns(msisdns)
        
        # Handle CSV output
        if isinstance(response, dict) and response.get("format") == "csv":
            csv_string = response.get("csv_string", "")
            filename = f"bulk_msisdn_validation_{response.get('summary', {}).get('timestamp', 'results')}.csv"
            
            return Response(
                csv_string,
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"[API] Bulk MSISDN validation error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@validator_bp.route('/validate/bulk/status', methods=['GET'])
def get_bulk_status():
    """Get status of bulk validation operations"""
    try:
        logger.info("[API] GET /api/validate/bulk/status")
        
        # Return basic status information
        status_info = {
            "status": "available",
            "bulk_operations_supported": hasattr(validator_service, 'validate_bulk_yesterday'),
            "timestamp": datetime.now().isoformat() + "Z"
        }
        
        return jsonify(status_info), 200
        
    except Exception as e:
        logger.error(f"[API] Bulk status error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500