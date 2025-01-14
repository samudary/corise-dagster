import csv
from datetime import datetime
from heapq import nlargest
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
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


# @usable_as_dagster_type(description="An object for communicating data state")
# class Result(BaseModel):
#     empty: boolean

def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(out={"with_stock_results": Out(is_required=False), "without_stock_results": Out(is_required=False)})
def get_s3_data_op():
    s3_key = context.op_config["s3_key"]
    stocks = [*csv_helper(s3_key)]

    if not stocks:
        yield Output(None, "without_stock_results")
    else:
        yield Output(stocks, "with_stock_results")


@op
def process_data_op():
    pass


@op
def put_redis_data_op():
    pass


@op
def put_s3_data_op():
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    out=Out(Nothing),
    description="Notifiy if stock list is empty",
)
def empty_stock_notify_op(context: OpExecutionContext, empty_stocks: Any):
    context.log.info("No stocks returned")


@job
def machine_learning_dynamic_job():
    stock_results, empty_stocks = get_s3_data_op()
    empty_stock_notify_op(empty_stocks)
    # process_data_op(stock_results)
