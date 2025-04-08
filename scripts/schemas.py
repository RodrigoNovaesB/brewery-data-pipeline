import pandera as pa
from pandera.typing import DataFrame

class BronzeSchema(pa.SchemaModel):
    id: pa.typing.Series[str]
    name: pa.typing.Series[str] 
    brewery_type: pa.typing.Series[str] = pa.Field(isin=["micro", "nano", "regional", "brewpub", "large"])
    street: pa.typing.Series[str]
    city: pa.typing.Series[str]
    state: pa.typing.Series[str] = pa.Field(str_length=2)
    postal_code: pa.typing.Series[str]
    country: pa.typing.Series[str]

    class Config:
        coerce = True
        strict = True

class GoldSchema(pa.SchemaModel):
    brewery_type: pa.typing.Series[str] = pa.Field(isin=["micro", "nano", "regional", "brewpub", "large"])
    state: pa.typing.Series[str] = pa.Field(str_length=2)
    count: pa.typing.Series[int] = pa.Field(ge=1)

    class Config:
        coerce = True