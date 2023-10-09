import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class RemoveUTC(beam.DoFn):
    def process(self, element):
        # Clone the row to keep the original data
        transformed_row = list(element)
        # Remove ' UTC' from the timestamp string
        timestamp_str = transformed_row[1].strip().replace(' UTC', '')
        transformed_row[1] = timestamp_str  # Update the timestamp in the row
        yield transformed_row  # Yield the updated row with all columns

# Define your pipeline options (e.g., runner, project, etc.)
options = PipelineOptions()

# Create a Dataflow pipeline
with beam.Pipeline(options=options) as p:
    # Read the CSV data and split it into columns
    csv_data = (p
                | beam.io.ReadFromText('gs://ayoubstockdata/data.csv')
                | beam.Map(lambda row: row.split(',')))

    # Process the data to remove 'UTC' from the timestamp column while keeping all columns
    transformed_data = (csv_data
                        | beam.ParDo(RemoveUTC()))  # Remove 'UTC' from the timestamp

    # Write the entire transformed data (including all columns) to an output file
    (transformed_data
     | beam.Map(lambda row: ','.join(row))  # Combine all columns into a CSV row
     | beam.io.WriteToText('gs://ayoubstockdata/output_with_timestamp'))
