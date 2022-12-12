from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    TimestampType,
    IntegerType,
    DateType
)


ecommerce_schema = StructType([
    StructField('fullVisitorId',DoubleType()),
    StructField('channelGrouping' ,StringType()),
    StructField('time' ,TimestampType()),
    StructField('country',StringType()),
    StructField('city',StringType()),
    StructField('totalTransactionRevenue',DoubleType()),
    StructField('transactions' ,DoubleType()),
    StructField('timeOnSite',DoubleType()),
    StructField('pageviews' ,IntegerType()),
    StructField('sessionQualityDim',DoubleType()),
    StructField('date', DateType()),
    StructField('visitId',IntegerType()),
    StructField('type',StringType()),
    StructField('productRefundAmount' ,DoubleType()),
    StructField('productQuantity' ,DoubleType()),
    StructField('productPrice', IntegerType()),
    StructField('productRevenue',DoubleType()),
    StructField('productSKU',StringType()),
    StructField('v2ProductName',StringType()),
    StructField('v2ProductCategory',StringType()),
    StructField('productVariant',StringType()),
    StructField('currencyCode',StringType()),
    StructField('itemQuantity',DoubleType()),
    StructField('itemRevenue',DoubleType()),
    StructField('transactionRevenue',DoubleType()),
    StructField('transactionId',StringType()),
    StructField('pageTitle',StringType()),
    StructField('searchKeyword',DoubleType()),
    StructField('pagePathLevel1',StringType()),
    StructField('eCommerceAction_type',IntegerType()),
    StructField('eCommerceAction_step', IntegerType()),
    StructField('eCommerceAction_option',StringType())]
)