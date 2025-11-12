# order_fetcher.py
"""
Config-Driven Order Fetcher
Routes between two strategies based on configuration:
- Mode 1: "latest" - Get latest order (any type) per MSISDN
- Mode 2: "filtered" - Get latest order (specific type) per MSISDN
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class OrderFetcher:
    """Fetches orders based on config-driven strategy"""
    
    def __init__(self, sf_client):
        """
        Initialize with Salesforce client
        
        Args:
            sf_client: simple_salesforce Salesforce client instance
        """
        self.sf = sf_client
        logger.info("[OrderFetcher] Initialized")
    
    def get_orders(self, config: Dict) -> Dict[str, Dict]:
        """
        MAIN ENTRY POINT - Routes based on config
        
        Config controls behavior:
            fetch_strategy: "latest" or "filtered"
            order_reason_filter: Which reason to filter for (if filtered mode)
        
        Args:
            config: Configuration dictionary with order_fetcher settings
        
        Returns:
            Dict mapping MSISDN -> latest order for that MSISDN
        
        Raises:
            ValueError: If config is invalid
        """
        order_config = config.get("order_fetcher", {})
        strategy = order_config.get("fetch_strategy", "latest")
        
        logger.info(f"[OrderFetcher] Entry point | strategy={strategy}")
        
        if strategy == "latest":
            return self._get_latest_orders(config)
        
        elif strategy == "filtered":
            return self._get_filtered_orders(config)
        
        else:
            error_msg = f"[OrderFetcher] Unknown fetch_strategy: {strategy}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def _get_latest_orders(self, config: Dict) -> Dict[str, Dict]:
        """
        MODE 1: Get LATEST order per MSISDN (any type)
        
        No filtering - queries ALL orders from yesterday
        Groups by MSISDN and keeps only the latest per customer
        
        Args:
            config: Configuration dictionary
        
        Returns:
            Dict mapping MSISDN -> latest order dict
        """
        logger.info("[OrderFetcher] MODE 1: LATEST (no filter)")
        
        try:
            # Build SOQL query - get all orders from yesterday
            soql = """
                SELECT 
                    PR_MSISDN__c,
                    Order.vlocity_cmt__Reason__c,
                    Order.Id,
                    CreatedDate,
                    Order.vlocity_cmt__OrderStatus__c
                FROM OrderItem
                WHERE CreatedDate >= YESTERDAY
                AND CreatedDate < TODAY  AND Product2.vlocity_cmt__ParentClassCode__c = 'PR_B2C_Mobile_Line_Class' AND Product2.Name != 'SIM Card' AND PR_MSISDN__c !=Null and Order.vlocity_cmt__OrderStatus__c ='Activated'
                ORDER BY PR_MSISDN__c, CreatedDate DESC Limit 100
            """
            
            logger.debug(f"[OrderFetcher] Mode 1 SOQL: {soql.strip()}")
            
            # Execute query
            result = self.sf.query_all(soql)
            records = result.get("records", []) if isinstance(result, dict) else (result or [])
            
            logger.info(f"[OrderFetcher] Mode 1: Fetched {len(records)} total order items")
            
            if not records:
                logger.warning("[OrderFetcher] Mode 1: No records found")
                return {}
            
            # Group by MSISDN, keep only latest per MSISDN
            grouped = {}
            for i, order in enumerate(records):
                msisdn = order.get("PR_MSISDN__c")
                
                if not msisdn:
                    logger.warning(f"[OrderFetcher] Mode 1: Record {i} has no MSISDN")
                    continue
                
                # First occurrence is latest (due to ORDER BY CreatedDate DESC)
                if msisdn not in grouped:
                    grouped[msisdn] = order
                    if i < 3:  # Log first few
                        reason = order.get("Order", {}).get("vlocity_cmt__Reason__c", "N/A")
                        logger.debug(f"[OrderFetcher] Mode 1: MSISDN {msisdn} <- {reason}")
            
            logger.info(f"[OrderFetcher] Mode 1: Grouped into {len(grouped)} unique MSISDNs")
            return grouped
        
        except Exception as e:
            logger.error(f"[OrderFetcher] Mode 1 error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _get_filtered_orders(self, config: Dict) -> Dict[str, Dict]:
        """
        MODE 2: Get LATEST order per MSISDN (filtered by reason)
        
        Filters in SOQL query (most efficient!)
        Gets only orders with specified reason
        Groups by MSISDN and keeps only the latest per customer
        
        Args:
            config: Configuration dictionary
        
        Returns:
            Dict mapping MSISDN -> latest order dict (filtered)
        
        Raises:
            ValueError: If filter config is invalid
        """
        order_config = config.get("order_fetcher", {})
        reason_filter = order_config.get("order_reason_filter")
        
        logger.info(f"[OrderFetcher] MODE 2: FILTERED (reason={reason_filter})")
        
        # Validate filter is provided
        if not reason_filter:
            error_msg = "[OrderFetcher] Mode 2: order_reason_filter required for 'filtered' mode"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Validate reason is in approved list
        valid_reasons = order_config.get("valid_order_reasons", [])
        if valid_reasons and reason_filter not in valid_reasons:
            error_msg = f"[OrderFetcher] Mode 2: Invalid reason '{reason_filter}'. Valid: {valid_reasons}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"[OrderFetcher] Mode 2: Filter validated | reason={reason_filter}")
        
        try:
            # Build SOQL query - get filtered orders from yesterday
            # IMPORTANT: Filtering in SOQL is more efficient than filtering in code
            soql = f"""
                SELECT 
                    PR_MSISDN__c,
                    Order.vlocity_cmt__Reason__c,
                    Order.Id,
                    CreatedDate,
                    Order.vlocity_cmt__OrderStatus__c
                FROM OrderItem
                WHERE CreatedDate >= YESTERDAY
                AND CreatedDate < TODAY
                AND Order.vlocity_cmt__Reason__c = '{reason_filter}'
                ORDER BY PR_MSISDN__c, CreatedDate DESC
            """
            
            logger.debug(f"[OrderFetcher] Mode 2 SOQL: {soql.strip()}")
            
            # Execute query
            result = self.sf.query_all(soql)
            records = result.get("records", []) if isinstance(result, dict) else (result or [])
            
            logger.info(f"[OrderFetcher] Mode 2: Fetched {len(records)} {reason_filter} order items")
            
            if not records:
                logger.warning(f"[OrderFetcher] Mode 2: No {reason_filter} records found")
                return {}
            
            # Group by MSISDN, keep only latest per MSISDN
            grouped = {}
            for i, order in enumerate(records):
                msisdn = order.get("PR_MSISDN__c")
                
                if not msisdn:
                    logger.warning(f"[OrderFetcher] Mode 2: Record {i} has no MSISDN")
                    continue
                
                # First occurrence is latest (due to ORDER BY CreatedDate DESC)
                if msisdn not in grouped:
                    grouped[msisdn] = order
                    if i < 3:  # Log first few
                        logger.debug(f"[OrderFetcher] Mode 2: MSISDN {msisdn} <- {reason_filter}")
            
            logger.info(f"[OrderFetcher] Mode 2: Grouped into {len(grouped)} unique MSISDNs (filtered)")
            return grouped
        
        except Exception as e:
            logger.error(f"[OrderFetcher] Mode 2 error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def get_orders_batch_by_msisdns(self, msisdns: List[str]) -> List[Dict]:
        """
        Fetch orders for specific MSISDNs (used by asset fetcher)
        
        Args:
            msisdns: List of MSISDN strings
        
        Returns:
            List of order records
        """
        if not msisdns:
            return []
        
        logger.info(f"[OrderFetcher] Fetching orders for {len(msisdns)} MSISDNs")
        
        # Chunk MSISDNs if too many (SOQL IN limit is 4000)
        chunks = [msisdns[i:i+100] for i in range(0, len(msisdns), 100)]
        all_records = []
        
        for chunk in chunks:
            # Build IN clause safely
            in_values = ",".join([f"'{m}'" for m in chunk])
            
            soql = f"""
                SELECT PR_MSISDN__c, Order.Id
                FROM OrderItem
                WHERE PR_MSISDN__c IN ({in_values})
                LIMIT 10000
            """
            
            result = self.sf.query_all(soql)
            records = result.get("records", []) if isinstance(result, dict) else (result or [])
            all_records.extend(records)
        
        logger.info(f"[OrderFetcher] Batch fetch: Got {len(all_records)} records")
        return all_records