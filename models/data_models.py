from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

# ==================== REQUEST MODELS ====================

class ValidateOrderRequest(BaseModel):
    msisdn: str = Field(..., description="Mobile number to validate")
    order_type: Optional[str] = Field(None, description="Order type (optional)")
    
    class Config:
        example = {"msisdn": "12218071145"}

# ==================== RESPONSE MODELS ====================

class LatestOrder(BaseModel):
    order_id: str
    order_type: str
    created_date: str
    reason: Optional[str] = None
    status: Optional[str] = None
    orchestration_plan_id: Optional[str] = None
    created_by: Optional[str] = None
    billing_account: Optional[str] = None
    segment: Optional[str] = None
    payment_type: Optional[str] = None

class Charge(BaseModel):
    one_time: Optional[float] = None
    recurring: Optional[float] = None

class ValidationCheck(BaseModel):
    __root__: Dict[str, bool] = {}

class ChildAsset(BaseModel):
    id: str
    product_name: str
    provisioning_status: Optional[str] = None
    parent_id: Optional[str] = None
    validations: Dict[str, bool] = {}

class LineAsset(BaseModel):
    root_id: str
    line_product: str
    provisioning_status: str
    asset_reference_id: str
    charges: Charge
    child_assets: List[ChildAsset] = []
    validations: Dict[str, bool]

class DeviceAsset(BaseModel):
    device_product: str
    product_class: str
    provisioning_status: str
    linked_to: str
    root_id: Optional[str] = None
    child_assets: List[Any] = []
    validations: Dict[str, bool]

class Assets(BaseModel):
    all: List[Dict[str, Any]] = []
    lines: List[LineAsset] = []
    devices: List[DeviceAsset] = []

class BasicValidation(BaseModel):
    lines_present: bool
    device_linked: bool
    all_passed: bool

class OrderSpecificValidation(BaseModel):
    __root__: Dict[str, Dict[str, Any]] = {}

class Validations(BaseModel):
    basic: BasicValidation
    order_specific: Dict[str, Dict[str, Any]]

class Summary(BaseModel):
    validation_status: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    lines_count: int
    devices_count: int

class Miscellaneous(BaseModel):
    notes: str

class ValidatedMSISDN(BaseModel):
    msisdn: str
    latest_order: LatestOrder
    assets: Assets
    validations: Validations
    summary: Summary
    miscellaneous: Miscellaneous

class ValidationResponse(BaseModel):
    status: str
    date_validated: str
    timestamp: str
    validated_msisdns: List[ValidatedMSISDN]
    summary: Dict[str, Any]
