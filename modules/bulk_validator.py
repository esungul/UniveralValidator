# bulk_validator.py
import logging
import requests
import json
import csv
import time
from typing import Dict, List, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.order_fetcher import OrderFetcher
from modules.order_filter import OrderFilter

logger = logging.getLogger(__name__)

class BulkValidator:
    """Bulk validation service for yesterday's orders"""
    
    def __init__(self, sf_connection, config: Dict[str, Any], api_base_url: str = "http://localhost:5000"):
        self.sf = sf_connection
        self.config = config
        self.api_base_url = api_base_url
        self.order_fetcher = OrderFetcher(sf_connection)
        self.order_filter = OrderFilter()
        
    def validate_yesterday_orders(self) -> Dict[str, Any]:
        """
        Main bulk validation workflow
        """
        logger.info("🚀 Starting bulk validation for yesterday's orders")
        
        try:
            # Step 1: Fetch yesterday's orders
            logger.info("📋 Step 1: Fetching yesterday's orders...")
            orders = self.order_fetcher.get_orders(self.config)
            logger.info(f"✓ Fetched {len(orders)} total orders")
            
            # Step 2: Filter and group by MSISDN
            logger.info("📊 Step 2: Filtering and grouping orders...")
            order_list = list(orders.values())
            grouped_orders = self.order_filter.filter_and_group(order_list, self.config)
            logger.info(f"✓ Grouped into {len(grouped_orders)} unique MSISDNs")
            
            # Step 3: Validate each MSISDN
            logger.info("🔍 Step 3: Validating MSISDNs...")
            validation_results = self._validate_msisdns_batch(list(grouped_orders.keys()))
            
            # Step 4: Format output
            logger.info("📝 Step 4: Formatting results...")
            output_format = self.config.get("bulk_validation", {}).get("output_format", "json")
            results = self._format_results(grouped_orders, validation_results, output_format)
            
            logger.info("✅ Bulk validation completed successfully!")
            return results
            
        except Exception as e:
            logger.error(f"❌ Bulk validation failed: {e}")
            raise
    
    def _validate_msisdns_batch(self, msisdns: List[str]) -> Dict[str, Any]:
        """
        Validate multiple MSISDNs using the API endpoint
        """
        results = {}
        total = len(msisdns)
        
        # Get concurrency settings from config
        bulk_config = self.config.get("bulk_validation", {})
        max_workers = bulk_config.get("max_concurrent_requests", 5)
        delay_between_requests = bulk_config.get("delay_between_requests", 0.1)
        
        logger.info(f"🔄 Validating {total} MSISDNs with {max_workers} concurrent workers")
        
        def validate_single_msisdn(msisdn: str) -> tuple:
            """Validate a single MSISDN and return result"""
            try:
                time.sleep(delay_between_requests)  # Rate limiting
                response = requests.post(
                    f"{self.api_base_url}/api/validate",
                    json={"msisdn": msisdn},
                    timeout=30
                )
                
                if response.status_code == 200:
                    return msisdn, response.json()
                else:
                    logger.warning(f"API call failed for {msisdn}: {response.status_code}")
                    return msisdn, {"error": f"HTTP {response.status_code}", "message": response.text}
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {msisdn}: {e}")
                return msisdn, {"error": "Request failed", "message": str(e)}
            except Exception as e:
                logger.error(f"Unexpected error for {msisdn}: {e}")
                return msisdn, {"error": "Validation failed", "message": str(e)}
        
        # Use ThreadPoolExecutor for concurrent validation
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_msisdn = {
                executor.submit(validate_single_msisdn, msisdn): msisdn 
                for msisdn in msisdns
            }
            
            completed = 0
            for future in as_completed(future_to_msisdn):
                msisdn, result = future.result()
                results[msisdn] = result
                completed += 1
                
                if completed % 10 == 0 or completed == total:
                    logger.info(f"📊 Progress: {completed}/{total} ({completed/total*100:.1f}%)")
        
        return results
    
    def _format_results(self, grouped_orders: Dict[str, Any], 
                       validation_results: Dict[str, Any], 
                       output_format: str) -> Dict[str, Any]:
        """
        Format results based on output format configuration
        """
        # Prepare summary
        total_msisdns = len(grouped_orders)
        successful_validations = sum(1 for r in validation_results.values() 
                                   if r.get("status") != "error")
        failed_validations = total_msisdns - successful_validations
        
        summary = {
            "total_msisdns": total_msisdns,
            "successful_validations": successful_validations,
            "failed_validations": failed_validations,
            "success_rate": round(successful_validations / total_msisdns * 100, 2) if total_msisdns > 0 else 0,
            "timestamp": datetime.now().isoformat(),
            "output_format": output_format
        }
        
        # Combine order data with validation results
        detailed_results = {}
        for msisdn, order_data in grouped_orders.items():
            validation_data = validation_results.get(msisdn, {})
            
            detailed_results[msisdn] = {
                "order_data": order_data,
                "validation_result": validation_data,
                "order_reason": order_data.get("Order", {}).get("vlocity_cmt__Reason__c"),
                "order_status": order_data.get("Order", {}).get("vlocity_cmt__Status__c"),
                "has_multiple_orders": order_data.get("has_multiple_orders", False),
                "order_count": order_data.get("order_count", 1)
            }
        
        if output_format.lower() == "csv":
            return self._convert_to_csv(summary, detailed_results)
        else:
            return {
                "summary": summary,
                "results": detailed_results
            }
    
    def _convert_to_csv(self, summary: Dict[str, Any], detailed_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert results to CSV format
        """
        csv_data = []
        
        # CSV header
        header = [
            "MSISDN", "Order Reason", "Order Status", "Validation Status", 
            "Has Multiple Orders", "Order Count", "Success Rate", "Errors"
        ]
        
        # CSV rows
        for msisdn, data in detailed_results.items():
            validation_result = data["validation_result"]
            validation_status = validation_result.get("status", "unknown")
            
            # Extract validation success rate
            success_rate = "N/A"
            if validation_status == "success":
                summary_data = validation_result.get("summary", {})
                success_rate = f"{summary_data.get('success_rate', 0)}%"
            
            # Extract errors
            errors = ""
            if validation_status == "error":
                errors = validation_result.get("message", "Validation failed")
            
            row = [
                msisdn,
                data.get("order_reason", "N/A"),
                data.get("order_status", "N/A"),
                validation_status,
                str(data.get("has_multiple_orders", False)),
                str(data.get("order_count", 1)),
                success_rate,
                errors
            ]
            csv_data.append(row)
        
        # Create CSV string
        csv_output = [header] + csv_data
        
        return {
            "summary": summary,
            "format": "csv",
            "csv_data": csv_output,
            "csv_string": self._csv_to_string(csv_output)
        }
    
    def _csv_to_string(self, csv_data: List[List[str]]) -> str:
        """Convert CSV data to string"""
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(csv_data)
        return output.getvalue()