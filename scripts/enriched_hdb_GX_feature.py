import great_expectations as gx
from great_expectations import expectations as gxe
import warnings
warnings.filterwarnings("ignore")

#1. Initialize your context 
context = gx.get_context()

# 2. Define your BigQuery Connection String
# Format: bigquery://project-id/dataset-name
connection_string = "bigquery://project-8d552288-1acb-4a23-893/hdb_raw_staging"

# 3. Add the BigQuery Data Source 
# Registers BigQuery as a SQL data source.
data_source_name = "hdb_raw_staging_source"
data_source = context.data_sources.add_sql(
    name=data_source_name,
    connection_string=connection_string
)

# 4. Define the asset (points to your physical table) and batch (table rows)
# add_table_asset: Points GX to specific table to test
raw_asset = data_source.add_table_asset(
    name="raw_enriched_transactions_asset",
    table_name="raw_enriched_transactions"    # confirmed in sources.yml
)
raw_batch = raw_asset.add_batch_definition_whole_table(
    name="raw_enriched_transactions_full_table" #use full table
)

#5. Define the suite  
suite = context.suites.add(
    gx.ExpectationSuite(name="raw_enriched_transactions_suite")
    )

# 6.0 Expectations: validate the raw columns that dbt will later transform -------

#6.1 dim_properties - 
# full_address: md5(full_address) used as property_id
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="full_address"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="town"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="flat_type"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="remaining_lease"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="month"))

#6.2 dim_school_proximity - md5(concat(closest_primary_school_name, primary_schools_within_1km))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="closest_primary_school_name"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="primary_schools_within_1km"))

#6.3 dim_retail_proximity- md5(concat(closest_shopping_mall_name, malls_within_1km))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="closest_shopping_mall_name"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="malls_within_1km"))

#6.4 dim_transit_proximity - md5(concat(closest_mrt_name, mrt_within_500m, lrt_within_500m))
# coalesce used in SQL so nulls are handled - skip not_null, just check column exists via table expectation
suite.add_expectation(gxe.ExpectColumnToExist(column="closest_mrt_name"))
suite.add_expectation(gxe.ExpectColumnToExist(column="mrt_within_500m"))
suite.add_expectation(gxe.ExpectColumnToExist(column="lrt_within_500m"))

#6.5 fact_sales - dedup partition columns
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="resale_price"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="floor_area_sqm"))

#7.0  --------------Run ----------------
val = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="raw_enriched_transactions_validation",
        suite=suite,
        data=raw_batch
    )
)
print(val.run())