import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    description="Get a list of stock data from an S3 file",
    out={"stocks": Out(dagster_type=List, is_required=True, description="Stock trading data")}
)
def get_s3_data_op(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [*csv_helper(s3_key)] # Mimic the use of a key to fetch a file from S3

@op(
    description="Given a list of stocks, returns an Aggregation with the highest value",
    out={"highest_stock_value": Out(dagster_type=Aggregation, is_required=True, description="Stock with the greatest 'high' value")}
)
def process_data_op(context, stocks: List) -> Aggregation:
    stock = max(stocks, key = lambda k: k.high)

    return Aggregation(date=stock.date, high=stock.high)


@op(
    description="Uploads aggregation data to Redis"
)
def put_redis_data_op(context, stock: Aggregation):
    pass


@op(
    description="Uploads aggregation data to S3"
)
def put_s3_data_op(context, stock: Aggregation):
    pass


@job
def machine_learning_job():
    processed_result = process_data_op(get_s3_data_op())
    put_s3_data_op(processed_result)
    put_redis_data_op(processed_result)
