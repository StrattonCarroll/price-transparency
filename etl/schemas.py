from __future__ import annotations
from typing import List, Optional, Union
from pydantic import BaseModel, Field, ConfigDict, validator
import datetime

# Based on the CMS.gov Hospital Price Transparency schema
# See: https://github.com/CMSgov/hospital-price-transparency/tree/master/documentation

class Code(BaseModel):
    """Represents a single billing code."""
    model_config = ConfigDict(populate_by_name=True)
    code_type: Optional[str] = Field(None, alias='billing_code_type')
    code: Optional[str] = Field(None, alias='billing_code')

class PayerRate(BaseModel):
    """Represents the rate negotiated with a specific payer for a service."""
    model_config = ConfigDict(populate_by_name=True)
    payer_name: str = Field(..., alias='payer_name')
    plan_name: Optional[str] = Field(None, alias='plan_name')
    negotiated_rate: float = Field(..., alias='negotiated_rate')
    # According to schema, this can be 'percentage', 'dollar', etc.
    negotiated_type: str = Field(..., alias='negotiated_type')

class StandardCharge(BaseModel):
    """
    Represents a single standard charge entry for an item or service.
    This is the core object that will be mapped to a row in our database.
    """
    model_config = ConfigDict(populate_by_name=True)
    description: str = Field(..., alias='description')
    codes: Optional[List[Code]] = Field(None, alias='billing_code_information')
    
    # Standard charge types
    gross_charge: Optional[float] = Field(None, alias='gross_charge')
    discounted_cash_charge: Optional[float] = Field(None, alias='discounted_cash_charge')
    
    # Payer-specific rates
    payer_rates: Optional[List[PayerRate]] = Field(None, alias='payer_negotiated_rates')

    # Optional fields from the schema
    setting: Optional[str] = None
    billing_class: Optional[str] = None
    modifiers: Optional[List[str]] = None
    drug_info: Optional[dict] = Field(None, alias='drug_information')
    
    # Our own metadata
    source_file: str

class HospitalTransparencyFile(BaseModel):
    """
    Represents the root JSON object for a hospital's machine-readable file.
    """
    hospital_name: str
    hospital_location: Optional[str] = None
    last_updated_on: datetime.date
    version: Optional[str]
    
    standard_charges: List[StandardCharge] = Field(..., alias='items_and_services')

    model_config = ConfigDict(populate_by_name=True)

    @validator('last_updated_on', pre=True)
    def parse_date(cls, value):
        if isinstance(value, str):
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        return value
